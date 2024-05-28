using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Reflection;


namespace Services.IntegrationService
{
    /// <summary>
    /// Manages functional processors of the Integration Service
    /// </summary>
    public class ProcessorManager : IProcessorManager
    {
        #region Constants and variables

        private volatile bool _disposed;

        private readonly IIntegrationService _integrationService;
        private ProcessorManagerParameters _parameters;
        private readonly object _parametersLock;

        private readonly ConcurrentDictionary<ProcessorType, IProcessor> _processors;

        internal event EventHandler<ProcessorMessageEventArgs> OnProcessorMessage;
        internal event ClientNotifyEventHandler OnClientNotify;

        #endregion

        #region Class methods

        internal ProcessorManager(ProcessorManagerParameters parameters)
        {
            // Obtain reference to main service class
            _integrationService = AppDomain.CurrentDomain.GetData("IntegrationService") as IIntegrationService;
            if (_integrationService == null)
                throw new ProcessorManagerException("Reference to main service class was not obtained");

            // Save this object to the AppDomain data, so all processors can access the manager
            AppDomain.CurrentDomain.SetData("IntegrationServiceProcessorManager", this);

            _processors = new ConcurrentDictionary<ProcessorType, IProcessor>();

            _parametersLock = new object();
            lock (_parametersLock)
            {
                _parameters = CloneHelper.Clone(parameters);

                if (_parameters == null || !_parameters.ProcessorsParameters.Any())
                    ServiceLogger.Warning("No processor is configured");
                else
                    CreateProcessors();
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        private void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                // Free the unmanaged sources
                DisposeProcessors();

                AppDomain.CurrentDomain.SetData("IntegrationServiceProcessorManager", null);

                if (disposing)
                {
                    // Free the managed sources
                }
                _disposed = true;
            }
        }

        ~ProcessorManager()
        {
            Dispose(false);
        }

        #endregion

        #region Private working methods

        private void CreateProcessors()
        {
            // Create list of processors which can access the list before cache sync
            var initialAccessProcessors = new List<ProcessorType>();
            foreach (var parameters in _parameters.ProcessorsParameters)
            {
                // Now only Asrun supports this functionality
                if (parameters.Type == ProcessorType.AsRun && _integrationService.LicensePolicy.EnableTrafficAsRun)
                {
                    initialAccessProcessors.Add(ProcessorType.AsRun);
                    break;
                }
            }

            // Add processors depending on the config values
            string strProcessor = typeof(ProcessorType).Name.Replace("Type", "");
            foreach (var parameters in _parameters.ProcessorsParameters)
            {
                string strProcessorType = Enum.GetName(typeof(ProcessorType), parameters.Type);

                // Check the processor is allowed to run by the license
                if (parameters.Type == ProcessorType.AsRun && !_integrationService.LicensePolicy.EnableTrafficAsRun ||
                    parameters.Type == ProcessorType.Branding && !_integrationService.LicensePolicy.EnableBrandingIntegration)
                {
                    ServiceLogger.Warning(strProcessorType + strProcessor + " was not created, it is not licensed");
                    continue;
                }

                CreateProcessor(parameters, initialAccessProcessors);
            }
        }

        private void CreateProcessor(ProcessorParameters parameters, List<ProcessorType> initialAccessProcessors)
        {
            const string errorMessage = " parameters were not found in appropriate config-section";
            string strProcessor = typeof(ProcessorType).Name.Replace("Type", "");
            string strProcessorType = Enum.GetName(typeof(ProcessorType), parameters.Type);

            // Extend the parameters of processors
            if (parameters.Type == ProcessorType.Playlist)
            {
                if (parameters is PlaylistProcessorParameters playlistProcessorParameters)
                    playlistProcessorParameters.InitialListHandlers = initialAccessProcessors;
            }

            string processorAssembly = IntegrationServiceAppConfiguration.Settings.RuntimeLoadableClasses[
                strProcessorType + strProcessor].AsssemblyPath;
            string processorClassName = IntegrationServiceAppConfiguration.Settings.RuntimeLoadableClasses[
                strProcessorType + strProcessor].ClassName;
            if (string.IsNullOrWhiteSpace(processorAssembly) || string.IsNullOrWhiteSpace(processorClassName))
                throw new ProcessorException(strProcessor + errorMessage);

            dynamic newProcessor = RuntimeUtils.LoadClassFromAssembly(
                processorAssembly, processorClassName, new object[] { parameters, _parameters.ConfiguredChannels });

            if (newProcessor == null)
                throw new ProcessorManagerException(strProcessorType + strProcessor + " was not created");
            newProcessor.OnProcessorMessage += new EventHandler<ProcessorMessageEventArgs>(OnProcessorMessageInvoke);

            var clientNotifyEvent = newProcessor.GetType().GetEvent("OnClientNotify");
            if (clientNotifyEvent != null)
                newProcessor.OnClientNotify += new ClientNotifyEventHandler(OnClientNotifyInvoke);

            _processors.TryAdd(parameters.Type, newProcessor);

            ServiceLogger.Informational(strProcessorType + strProcessor + " was created");
        }

        private void DisposeProcessors()
        {
            foreach (var processorPair in _processors)
            {
                if (processorPair.Value is IDisposable processor)
                    processor.Dispose();
            }
            _processors.Clear();
        }

        private Type GetProcessorClassType(ProcessorType userType)
        {
            string strProcessor = typeof(ProcessorType).Name.Replace("Type", "");
            string strProcessorUserType = Enum.GetName(typeof(ProcessorType), userType);
            string processorClassName = IntegrationServiceAppConfiguration.Settings.RuntimeLoadableClasses[
                strProcessorUserType + strProcessor].ClassName;
            string processorAssembly = IntegrationServiceAppConfiguration.Settings.RuntimeLoadableClasses[
                strProcessorUserType + strProcessor].AsssemblyPath;

            return Type.GetType(processorClassName + "," + processorAssembly.Replace(".dll", ""));
        }

        #endregion

        #region Internal methods

        /// <summary>
        /// Execute a specific command on a processor
        /// </summary>
        /// <param name="commandType">Type of the command to a processor</param>
        /// <param name="commandParameters">Parameters of the command</param>
        /// <returns>Result of the command execution</returns>
        internal object ExecuteProcessorCommand(ProcessorCommandType commandType, params object[] commandParameters)
        {
            string methodName = Enum.GetName(typeof(ProcessorCommandType), commandType);

            switch (commandType)
            {
                // Add message to the processing queue of the processor
                case ProcessorCommandType.AddProcessingMessage:
                    {
                        // Initialize ProcessorType
                        ProcessorType messageProcessorType;
                        if ((commandParameters != null) &&
                            (commandParameters.Length == 1) && (commandParameters[0] is ProcessingMessage))
                        {
                            try
                            {
                                messageProcessorType = ProcessorMessageInterfaceMatch.GetProcessorTypeForMessage(
                                    ((ProcessingMessage)commandParameters[0]).Message.First().MessageType);
                            }
                            catch (MessageCoreException ex)
                            {
                                throw new ProcessorManagerException("No processor for the message '" +
                                    ((ProcessingMessage)commandParameters[0]).Message.First().MessageType + "' is defined", ex);
                            }
                        }
                        else if ((commandParameters != null) &&
                                 (commandParameters.Length == 2) && (commandParameters[0] is ProcessorType) &&
                                 (commandParameters[1] is ProcessingMessage))
                        {
                            messageProcessorType = (ProcessorType)commandParameters[0];
                        }
                        else
                        {
                            throw new ProcessorManagerException("Invalid parameters were provided to the '" +
                                                                methodName + "' processor command");
                        }

                        // Find a proper message processor
                        IProcessor messageProcessor;
                        if (!_processors.TryGetValue(messageProcessorType, out messageProcessor))
                        {
                            throw new ProcessorManagerException("No processor for the message '" +
                                ((ProcessingMessage)commandParameters[0]).Message.First().MessageType + "' is configured now");
                        }

                        // Add message to the processor queue
                        try
                        {
                            messageProcessor.AddProcessingMessage((ProcessingMessage)commandParameters[0]);
                        }
                        catch (ProcessorNotInitializedException ex)
                        {
                            throw new ProcessorManagerException("The proper processor for the message '" +
                                ((ProcessingMessage)commandParameters[0]).Message.First().MessageType + "' is not initialized at this moment", ex);
                        }
                        catch (ProcessorException ex)
                        {
                            throw new ProcessorManagerException("The message processor has thrown an exception", ex);
                        }

                        break;
                    }

                // Invoke the Playlist processor for obtaining the schedule changes
                case ProcessorCommandType.GetScheduleChanges:
                    {
                        break;
                    }

                // Invoke the Playlist processor for applying/rejecting the message 
                case ProcessorCommandType.ScheduleChangesApprovalResult:
                    {
                        IProcessor messageProcessor;
                        if (!_processors.TryGetValue(ProcessorType.Playlist, out messageProcessor))
                        {
                            throw new ProcessorManagerException("No Playlist processor is configured now");
                        }

                        // Validate processor class name
                        Type messageProcessorType = GetProcessorClassType(ProcessorType.Playlist);
                        if (messageProcessor.GetType() == messageProcessorType)
                        {
                            // Validate parameters
                            if ((commandParameters != null) && (commandParameters.Length == 2) &&
                                (commandParameters[0] is Guid) && (commandParameters[1] is ApprovalType))
                            {
                                // Execute processor
                                messageProcessorType.InvokeMember(methodName,
                                    BindingFlags.Instance | BindingFlags.InvokeMethod | BindingFlags.Public,
                                    Type.DefaultBinder,
                                    messageProcessor, commandParameters);
                            }
                            else
                            {
                                throw new ProcessorManagerException("Invalid parameters were provided to the '" +
                                                                    methodName + "' command of the Playlist processor");
                            }
                        }
                        else
                        {
                            throw new ProcessorManagerException("Playlist processor has incorrect class type");
                        }

                        break;
                    }

                case ProcessorCommandType.IncreaseApprovalTimeout:
                    {
                        IProcessor messageProcessor;
                        if (!_processors.TryGetValue(ProcessorType.Playlist, out messageProcessor))
                        {
                            throw new ProcessorManagerException("No Playlist processor is configured now");
                        }

                        // Validate processor class name
                        Type messageProcessorType = GetProcessorClassType(ProcessorType.Playlist);
                        if (messageProcessor.GetType() == messageProcessorType)
                        {
                            // Validate parameters
                            if ((commandParameters != null) && (commandParameters.Length == 1) && (commandParameters[0] is Guid))
                            {
                                // Execute processor
                                messageProcessorType.InvokeMember(methodName,
                                    BindingFlags.Instance | BindingFlags.InvokeMethod | BindingFlags.Public,
                                    Type.DefaultBinder,
                                    messageProcessor, commandParameters);
                            }
                            else
                            {
                                throw new ProcessorManagerException("Invalid parameters were provided to the '" +
                                                                    methodName + "' command of the Playlist processor");
                            }
                        }
                        else
                        {
                            throw new ProcessorManagerException("Playlist processor has incorrect class type");
                        }
                        break;
                    }

                // Invoke the handler for applying/rejecting the cache restore
                case ProcessorCommandType.CachRestoreApprovalResult:
                    {
                        IProcessor messageProcessor;
                        if (!_processors.TryGetValue(ProcessorType.Playlist, out messageProcessor))
                        {
                            throw new ProcessorManagerException("No Playlist processor is configured now");
                        }

                        // Validate processor class name
                        Type messageProcessorType = GetProcessorClassType(ProcessorType.Playlist);
                        if (messageProcessor.GetType() == messageProcessorType)
                        {
                            // Validate parameters
                            if ((commandParameters != null) && (commandParameters.Length == 1) &&
                                (commandParameters[0] is ApprovalType))
                            {
                                // Execute processor
                                messageProcessorType.InvokeMember(methodName,
                                    BindingFlags.Instance | BindingFlags.InvokeMethod | BindingFlags.Public,
                                    Type.DefaultBinder,
                                    messageProcessor, commandParameters);
                            }
                            else
                            {
                                throw new ProcessorManagerException("Invalid parameters were provided to the '" +
                                                                    methodName + "' command of the Playlist processor");
                            }
                        }
                        else
                        {
                            throw new ProcessorManagerException("Playlist processor has incorrect class type");
                        }

                        break;
                    }
            }

            return null;
        }

        /// <summary>
        /// Callback from processors
        /// </summary>
        internal void OnProcessorMessageInvoke(object sender, ProcessorMessageEventArgs e)
        {
            if (OnProcessorMessage != null)
                OnProcessorMessage(sender, e);
        }

        /// <summary>
        /// Callback from processors to WCF clients
        /// </summary>
        internal object OnClientNotifyInvoke(object sender, IntegrationServiceEventArgs e)
        {
            if (OnClientNotify != null)
                return OnClientNotify(sender, e);
            return null;
        }

        private bool RenameChannel(string channelCurrentName, string channelNewName)
        {
            bool result = true;

            foreach (var channelProcessor in _processors.Values.OfType<IChannelProcessor>())
            {
                if (!channelProcessor.RenameChannel(channelCurrentName, channelNewName))
                {
                    result = false;
                    break;
                }
            }

            return result;
        }

        internal void UpdateConfiguration(Dictionary<string, ConfigurationPropertyNode> changedProperties)
        {
            lock (_parametersLock)
            {
                bool onlyNameIsChanged = false;
                string channelNewName = "";
                string channelOldName = "";

                if (changedProperties != null && changedProperties.ContainsKey("ConfiguredChannels"))
                {
                    var configuredChannels = changedProperties["ConfiguredChannels"].Children;
                    if (configuredChannels != null && configuredChannels.Count == 1)
                    {
                        var configuredChannel = configuredChannels.First().Value.Children;
                        if (configuredChannel != null && configuredChannel.Count == 1 &&
                            configuredChannel.ContainsKey("Name"))
                        {
                            onlyNameIsChanged = true;
                            channelNewName = (string)configuredChannel["Name"].NewValue;
                            channelOldName = (string)configuredChannel["Name"].OldValue;
                        }
                    }
                }

                bool renameResult = false;
                if (onlyNameIsChanged)
                {
                    renameResult = RenameChannel(channelOldName, channelNewName);
                    _parameters = CloneHelper.Clone(Config.Instance.ConfigObject.ProcessorsManagerParameters);
                }

                if (!renameResult)
                {
                    if (changedProperties != null && changedProperties.ContainsKey("ProcessorsParameters"))
                    {
                        var processorParameters = changedProperties["ProcessorsParameters"].Children;
                        if (processorParameters != null)
                        {
                            string processorPlaylistKey = null;
                            if (_parameters != null)
                            {
                                var processor = _parameters.ProcessorsParameters.FirstOrDefault(item => item.Type == ProcessorType.Playlist);
                                if (processor != null)
                                {
                                    int processorItemIndex = _parameters.ProcessorsParameters.IndexOf(processor);
                                    if (processorItemIndex != -1)
                                        processorPlaylistKey = processorItemIndex.ToString(CultureInfo.InvariantCulture);
                                }
                            }

                            if (processorPlaylistKey != null && processorParameters.ContainsKey(processorPlaylistKey))
                            {
                                IProcessor playlistProcessor;
                                if (_processors.TryGetValue(ProcessorType.Playlist, out playlistProcessor))
                                {
                                    if (!playlistProcessor.UpdateConfiguration(processorParameters[processorPlaylistKey].Children))
                                    {
                                        DisposeProcessors();

                                        _parameters = CloneHelper.Clone(Config.Instance.ConfigObject.ProcessorsManagerParameters);

                                        if (_parameters == null || !_parameters.ProcessorsParameters.Any())
                                            ServiceLogger.Warning("No processor is configured");
                                        else
                                            CreateProcessors();

                                        return;
                                    }
                                }
                            }

                            foreach (var processorParametersType in processorParameters.Keys)
                            {
                                if (processorParametersType == processorPlaylistKey)
                                    continue;

                                if (_parameters != null)
                                {
                                    int index;
                                    if (int.TryParse(processorParametersType, out index) &&
                                        (index >= 0) && (index < _parameters.ProcessorsParameters.Count))
                                    {
                                        var processorType = _parameters.ProcessorsParameters[index].Type;

                                        IProcessor processor;
                                        if (_processors.TryGetValue(processorType, out processor))
                                            processor.UpdateConfiguration(processorParameters[processorParametersType].Children);
                                    }
                                }
                            }
                        }
                        else
                        {
                            DisposeProcessors();

                            _parameters = CloneHelper.Clone(Config.Instance.ConfigObject.ProcessorsManagerParameters);

                            if (_parameters == null || !_parameters.ProcessorsParameters.Any())
                                ServiceLogger.Warning("No processor is configured");
                            else
                                CreateProcessors();
                        }
                    }
                }
            }
        }

        internal void UpdateLicensedFunctionality()
        {
            lock (_parametersLock)
            {
                // Apply the IntegrationServiceListLimit value
                IProcessor playlistProcessor;
                if (_processors.TryGetValue(ProcessorType.Playlist, out playlistProcessor))
                {
                    // If license decreases the number of channels this will return true
                    if (!playlistProcessor.UpdateLicensedFunctionality())
                    {
                        // If we can add more channels, re-create all processors because 
                        // re-creation of only the PlaylistProcessor might disrupt the state of other processors
                        DisposeProcessors();
                        if (_parameters == null || !_parameters.ProcessorsParameters.Any())
                            ServiceLogger.Warning("No processor is configured");
                        else
                            CreateProcessors();

                        // After all processors are re-created we are sure that 
                        // the rest of the license parametes are applied during creation
                        return;
                    }
                }

                // Apply the EnableTrafficAsRun value
                IProcessor asrunProcessor;
                bool asrunProcessorExists = _processors.TryGetValue(ProcessorType.AsRun, out asrunProcessor);

                if (!_integrationService.LicensePolicy.EnableTrafficAsRun && asrunProcessorExists)
                {
                    // Stop the AsRun processor
                    ServiceLogger.Debug("UpdateLicensedFunctionality: stopping the AsRun processor");

                    IProcessor processor;
                    _processors.TryRemove(ProcessorType.AsRun, out processor);
                    if (processor is IDisposable processorDisposable)
                        processorDisposable.Dispose();
                }
                else if (_integrationService.LicensePolicy.EnableTrafficAsRun && !asrunProcessorExists)
                {
                    if (_parameters != null)
                    {
                        // Start the AsRun processor
                        var processorParameters = _parameters.ProcessorsParameters.SingleOrDefault(item => item.Type == ProcessorType.AsRun);
                        if (processorParameters != null)
                        {
                            ServiceLogger.Debug("UpdateLicensedFunctionality: starting the AsRun processor");
                            CreateProcessor(processorParameters, null);
                        }
                    }
                }
                else if (_integrationService.LicensePolicy.EnableTrafficAsRun && asrunProcessorExists)
                {
                    // Possibly reduce the number of channels allowed by the new license
                    asrunProcessor.UpdateLicensedFunctionality();
                }

                // Apply the EnableBrandingIntegration value
                IProcessor brandingProcessor;
                bool brandingProcessorExists = _processors.TryGetValue(ProcessorType.Branding, out brandingProcessor);

                if (!_integrationService.LicensePolicy.EnableBrandingIntegration && brandingProcessorExists)
                {
                    // Stop the Branding processor
                    ServiceLogger.Debug("UpdateLicensedFunctionality: stopping the Branding processor");

                    IProcessor processor;
                    _processors.TryRemove(ProcessorType.Branding, out processor);
                    if (processor is IDisposable processorDisposable)
                        processorDisposable.Dispose();
                }
                else if (_integrationService.LicensePolicy.EnableBrandingIntegration && !brandingProcessorExists)
                {
                    if (_parameters != null)
                    {
                        // Start the Branding processor
                        var processorParameters = _parameters.ProcessorsParameters.SingleOrDefault(item => item.Type == ProcessorType.Branding);
                        if (processorParameters != null)
                        {
                            ServiceLogger.Debug("UpdateLicensedFunctionality: starting the Branding processor");
                            CreateProcessor(processorParameters, null);
                        }
                    }
                }
                else if (_integrationService.LicensePolicy.EnableBrandingIntegration && brandingProcessorExists)
                {
                    // Possibly reduce the number of channels allowed by the new license
                    brandingProcessor.UpdateLicensedFunctionality();
                }
            }
        }

        #endregion

        #region Public methods

        public IProcessor GetProcessorByType(ProcessorType processorType)
        {
            IProcessor result;
            if (_processors.TryGetValue(processorType, out result))
                return result;
            return null;
        }

        #endregion
    }
}

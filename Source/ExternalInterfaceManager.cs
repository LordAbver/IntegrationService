using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace Services.IntegrationService
{
    /// <summary>
    /// Manages the interfaces to the external systems
    /// </summary>
    internal class ExternalInterfaceManager : IDisposable
    {
        #region Constants and variables

        private volatile bool _disposed;
        private readonly IIntegrationService _integrationService;
        private ExternalInterfaceManagerParameters _parameters;
        private readonly object _parametersLock;
        private readonly ConcurrentDictionary<CommunicationInterfaceType, ICommunicationInterface> _communicationInterfaces;

        #endregion

        #region Class methods

        internal ExternalInterfaceManager(ExternalInterfaceManagerParameters parameters)
        {
            // Obtain reference to main service class
            _integrationService = AppDomain.CurrentDomain.GetData("IntegrationService") as IIntegrationService;
            if (_integrationService == null)
                throw new ExternalInterfaceManagerException("Reference to main service class was not obtained");

            _communicationInterfaces = new ConcurrentDictionary<CommunicationInterfaceType, ICommunicationInterface>();
            _parametersLock = new object();
            lock (_parametersLock)
            {
                _parameters = CloneHelper.Clone(parameters);

                if (_parameters == null || !_parameters.CommunicationInterfacesParameters.Any())
                {
                    ServiceLogger.Warning("No external interface is configured");
                }
                else
                {
                    CreateCommunicationInterfaces();
                }
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
                foreach (var communicationInterface in _communicationInterfaces)
                {
                    communicationInterface.Value.Dispose();
                }

                if (disposing)
                {
                    // Free the managed sources
                }
                _disposed = true;
            }
        }

        ~ExternalInterfaceManager()
        {
            Dispose(false);
        }

        #endregion

        #region Private working methods

        private void CreateCommunicationInterfaces()
        {
            string strCommunicationInterface = typeof(CommunicationInterfaceType).Name.Replace("Type", "");

            foreach (var parameters in _parameters.CommunicationInterfacesParameters)
            {
                string strCommunicationInterfaceType = Enum.GetName(typeof(CommunicationInterfaceType), parameters.Type);

                // Check license parameters
                if (parameters.Type == CommunicationInterfaceType.Bxf && !_integrationService.LicensePolicy.EnableTrafficCommunication ||
                    parameters.Type == CommunicationInterfaceType.Branding && !_integrationService.LicensePolicy.EnableBrandingIntegration)
                {
                    ServiceLogger.Warning(strCommunicationInterfaceType +
                                          strCommunicationInterface + " was not created, it is not licensed");
                    continue;
                }

                CreateCommunicationInterface(parameters);
            }
        }

        private void CreateCommunicationInterface(CommunicationInterfaceParameters parameters)
        {
            const string errorMessage = " parameters were not found in appropriate config-section";
            string strCommunicationInterface = typeof(CommunicationInterfaceType).Name.Replace("Type", "");
            string strCommunicationInterfaceType = Enum.GetName(typeof(CommunicationInterfaceType), parameters.Type);

            // Get assembly path
            string communicationInterfaceAssembly = IntegrationServiceAppConfiguration.Settings.RuntimeLoadableClasses[
                strCommunicationInterfaceType + strCommunicationInterface].AsssemblyPath;
            if (string.IsNullOrWhiteSpace(communicationInterfaceAssembly))
                throw new CommunicationInterfaceException(strCommunicationInterface + errorMessage);

            // Create instance
            if (parameters.ConnectionParameters != null && parameters.FormatterParameters != null)
            {
                var communicationInterface = (ICommunicationInterface)RuntimeUtils.LoadClassFromAssembly(
                    communicationInterfaceAssembly, typeof(ICommunicationInterface), new object[] { parameters });

                if (communicationInterface == null)
                {
                    throw new ExternalInterfaceManagerException(strCommunicationInterface + " " +
                                                                strCommunicationInterfaceType + " was not created");
                }

                _communicationInterfaces.TryAdd(parameters.Type, communicationInterface);

                ServiceLogger.Informational(strCommunicationInterfaceType +
                                            strCommunicationInterface + " was created");
            }
            else
            {
                ServiceLogger.Warning(strCommunicationInterfaceType +
                                      strCommunicationInterface + " was not created, configuration is missed");
            }
        }

        #endregion

        #region Internal methods

        /// <summary>
        /// Receive a message from the specified communication interface
        /// </summary>
        /// <param name="interfaceType">Interface to process the command</param>
        /// <returns>Parsed message or 'null' if no message is available on that interface</returns>
        internal IEnumerable<CommunicationItem> ReceiveMessage(CommunicationInterfaceType interfaceType)
        {
            ICommunicationInterface communicationInterface;
            if (!_communicationInterfaces.TryGetValue(interfaceType, out communicationInterface))
                return null;

            if (communicationInterface != null)
                return communicationInterface.ReceiveMessage();
            return null;
        }

        /// <summary>
        /// Send the message to the specified communication interface
        /// </summary>
        /// <param name="message">Message to send</param>
        /// <returns>'True' if the message is added to sending queue, 
        /// 'False' if the required interface is not configured</returns>
        internal void SendMessage(IEnumerable<CommunicationItem> message)
        {
            CommunicationInterfaceType interfaceType;
            try
            {
                interfaceType = ProcessorMessageInterfaceMatch.GetCommunicationInterfaceType(
                    message.First().MessageType);
            }
            catch (MessageCoreException ex)
            {
                throw new NoCommunicationInterfaceException("No communication interface for the message \"" +
                    message.First().MessageType + "\" is defined", ex);
            }

            ICommunicationInterface communicationInterface;
            if (!_communicationInterfaces.TryGetValue(interfaceType, out communicationInterface))
            {
                throw new NotAvailableCommunicationInterfaceException("No communication interface for the message \"" +
                    message.First().MessageType + "\" is available now");
            }

            try
            {
                communicationInterface.SendMessage(message);
            }
            catch (CommunicationInterfaceException ex)
            {
                throw new ExternalInterfaceManagerException("Error while adding the message to the outgoing queue", ex);
            }
        }

        /// <summary>
        /// Apply changed configuration
        /// </summary>
        internal void UpdateConfiguration(Dictionary<string, ConfigurationPropertyNode> changedProperties)
        {
            lock (_parametersLock)
            {
                ConfigurationPropertyNode configurationNode;
                if (changedProperties != null && changedProperties.TryGetValue("CommunicationInterfacesParameters", out configurationNode))
                {
                    var interfacesParameters = configurationNode.Children;
                    if (interfacesParameters != null)
                    {
                        foreach (var communicationInterface in _communicationInterfaces.Values.OfType<CommunicationInterface>())
                        {
                            communicationInterface.UpdateConfiguration(interfacesParameters);
                        }
                        ServiceLogger.Debug("UpdateConfiguration: communication interfaces are updated");

                        _parameters = CloneHelper.Clone(Config.Instance.ConfigObject.ExternalInterfacesManagerParameters);
                    }
                    else
                    {
                        foreach (var communicationInterface in _communicationInterfaces.Values)
                        {
                            communicationInterface.Dispose();
                        }
                        _communicationInterfaces.Clear();
                        ServiceLogger.Debug("UpdateConfiguration: communication interfaces are disposed");

                        _parameters = CloneHelper.Clone(Config.Instance.ConfigObject.ExternalInterfacesManagerParameters);

                        CreateCommunicationInterfaces();
                        ServiceLogger.Debug("UpdateConfiguration: communication interfaces are created");
                    }
                }
            }
        }

        /// <summary>
        /// Apply the changed license parameters to the license-enabled interfaces
        /// </summary>
        internal void UpdateLicensedFunctionality()
        {
            lock (_parametersLock)
            {
                // Apply the EnableTrafficCommunication value
                bool trafficInterfaceExists = _communicationInterfaces.ContainsKey(CommunicationInterfaceType.Bxf);

                if (!_integrationService.LicensePolicy.EnableTrafficCommunication && trafficInterfaceExists)
                {
                    // Stop the Traffic communication
                    ServiceLogger.Debug("UpdateLicensedFunctionality: stopping the Traffic communication");

                    ICommunicationInterface trafficInterface;
                    _communicationInterfaces.TryRemove(CommunicationInterfaceType.Bxf, out trafficInterface);
                    trafficInterface.Dispose();
                }
                else if (_integrationService.LicensePolicy.EnableTrafficCommunication && !trafficInterfaceExists)
                {
                    // Start the Traffic communication
                    CommunicationInterfaceParameters interfaceParameters = null;
                    if (_parameters != null)
                        interfaceParameters = _parameters.CommunicationInterfacesParameters.SingleOrDefault(
                            item => item.Type == CommunicationInterfaceType.Bxf);

                    if (interfaceParameters != null)
                    {
                        ServiceLogger.Debug("UpdateLicensedFunctionality: starting the Traffic communication");
                        CreateCommunicationInterface(interfaceParameters);
                    }
                }

                // Apply the EnableBrandingIntegration value
                bool brandingInterfaceExists =
                    _communicationInterfaces.ContainsKey(CommunicationInterfaceType.Branding);

                if (!_integrationService.LicensePolicy.EnableBrandingIntegration && brandingInterfaceExists)
                {
                    // Stop the Branding communication
                    ServiceLogger.Debug("UpdateLicensedFunctionality: stopping the Branding communication");

                    ICommunicationInterface brandingInterface;
                    _communicationInterfaces.TryRemove(CommunicationInterfaceType.Branding, out brandingInterface);
                    brandingInterface.Dispose();
                }
                else if (_integrationService.LicensePolicy.EnableBrandingIntegration && !brandingInterfaceExists)
                {
                    // Start the Branding communication
                    CommunicationInterfaceParameters interfaceParameters = null;
                    if (_parameters != null)
                        interfaceParameters = _parameters.CommunicationInterfacesParameters.SingleOrDefault(
                            item => item.Type == CommunicationInterfaceType.Branding);

                    if (interfaceParameters != null)
                    {
                        ServiceLogger.Debug("UpdateLicensedFunctionality: starting the Branding communication");
                        CreateCommunicationInterface(interfaceParameters);
                    }
                }
            }
        }

        #endregion
    }
}

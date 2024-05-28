using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Reflection;
using System.ServiceModel;
using System.Threading;

namespace Services.IntegrationService
{
    [ServiceBehavior(InstanceContextMode = InstanceContextMode.Single)]
    public class IntegrationService : BusinessService, IIntegrationService, IDisposable
    {
        #region Variables

        private volatile bool _disposed;
        private Thread _clientsMonitorThread;
        private List<AsyncIntegrationServiceClient> _subscribers;
        private object _subscribersLock;
        private Func<IIntegrationServiceClient> _callbackCreator;
        /// <summary>
        /// Core of the message processing
        /// </summary>
        private MessageCore _messageCore;
        private IInternalNotifier _internalNotification;
        private volatile CacheProcessStatus _cacheProcess;
        private volatile bool _applicationStart;

        #endregion

        #region Properties

        /// <summary>
        /// Common cache processing status
        /// </summary>
        public CacheProcessStatus CacheProcess
        {
            get { return _cacheProcess; }
            set { _cacheProcess = value; }
        }

        /// <summary>
        /// Signalize that the application is started
        /// </summary>
        public bool ApplicationStart
        {
            get { return _applicationStart; }
            set { _applicationStart = value; }
        }

        /// <summary>
        /// Provide license parameters
        /// </summary>
        public LicenseHelper LicensePolicy { get; private set; }

        #endregion

        #region Class methods

        public IntegrationService()
        {
            IntegrationServiceCreate(() => OperationContext.Current.GetCallbackChannel<IIntegrationServiceClient>());
        }

#if DEBUG
        public IntegrationService(Func<IIntegrationServiceClient> callbackCreator)
        {
            IntegrationServiceCreate(callbackCreator);
        }
#endif

        private void IntegrationServiceCreate(Func<IIntegrationServiceClient> callbackCreator)
        {
            if (callbackCreator == null)
                throw new ArgumentNullException("callbackCreator");

            LicensePolicy = new LicenseHelper();
            LicensePolicy.SecurityPolicyChanged += LicensePolicyChanged;
            if (!LicensePolicy.EnableIntegrationService)
            {
                LicensePolicy.Dispose();
                LicensePolicy = null;
                throw new PolicyViolationException("Integration Service is not licensed");
            }

            _subscribers = new List<AsyncIntegrationServiceClient>();
            _subscribersLock = new object();
            _callbackCreator = callbackCreator;

            InitializeIntegrationService();
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
                if (_clientsMonitorThread != null)
                {
                    _clientsMonitorThread.Interrupt();
                    _clientsMonitorThread.Join();
                    _clientsMonitorThread = null;
                }
                if (_messageCore != null)
                {
                    _messageCore.Dispose();
                    _messageCore = null;
                }
                if (_subscribersLock != null && _subscribers != null)
                {
                    lock (_subscribersLock)
                    {
                        foreach (var asyncServiceClient in _subscribers)
                        {
                            asyncServiceClient.Dispose();
                        }
                        _subscribers.Clear();
                        _subscribers = null;
                    }
                    _subscribersLock = null;
                }
                if (LicensePolicy != null)
                {
                    LicensePolicy.Dispose();
                    LicensePolicy = null;
                }

                AppDomain.CurrentDomain.SetData("IntegrationService", null);
                AppDomain.CurrentDomain.SetData("InternalNotification", null);
                _internalNotification = null;

                if (disposing)
                {
                }
                _disposed = true;
            }
        }

        ~IntegrationService()
        {
            Dispose(false);
        }

        #endregion

        #region Private and internal working methods

        private void InitializeIntegrationService()
        {
            ApplicationStart = true;

            // Save this object to the AppDomain data, so all parts of program will be able 
            // to send notifications to subscribers and report cache status
            AppDomain.CurrentDomain.SetData("IntegrationService", this);

            _internalNotification = new InternalNotifier();
            AppDomain.CurrentDomain.SetData("InternalNotification", _internalNotification);

            // Create config object and load configuration from specified file
            string folderPath = Path.GetDirectoryName(Assembly.GetExecutingAssembly().CodeBase);
            if (folderPath == null)
            {
                ServiceLogger.Warning("Integration Service was not able to get the " +
                                      "GetExecutingAssembly().CodeBase value. Starting with the default configuration values.");
            }
            else
            {
                string configurationFilePath;
                try
                {
                    configurationFilePath = ConfigurationManager.AppSettings["ConfigFilePath"];
                }
                catch (ConfigurationException)
                {
                    configurationFilePath = null;
                }

                if (configurationFilePath != null)
                    Config.Instance.Load(Path.Combine(folderPath, configurationFilePath));
                else
                {
                    ServiceLogger.Warning("Integration Service was not able to find the configuration file. " +
                                          "Starting with the default values.");
                }
            }
            // Subscribe to config file changes made by config object
            Config.Instance.SubscribeToAllConfigurationChanges(ConfigPropertyChanged);

            // Create MessageCore instance
            _messageCore = new MessageCore(Config.Instance.ConfigObject);
            _messageCore.OnClientNotify += OnClientNotifyInvoke;

            // Create clients monitor thread
            _clientsMonitorThread = new Thread(CheckClients) { Name = "ClientsMonitor" };
            _clientsMonitorThread.Start();
        }

        private void ConfigPropertyChanged(object sender, PropertyChangedEventArgs e)
        {
            // Some setting is changed, need to configure threads
            if (e.PropertyName == @"ConfigXML")
            {
                ServiceLogger.Debug("New changed configuration xml:", Config.Instance.ConfigXml);

                if (e is ConfigurationPropertyChangedEventArgs args && args.ChangedProperties != null)
                {
                    var configApplyThread = new Thread(() => _messageCore.UpdateConfiguration(args.ChangedProperties))
                    {
                        IsBackground = true,
                        Name = "ConfigApply"
                    };
                    configApplyThread.Start();
                }
            }
        }

        private void LicensePolicyChanged(object sender, EventArgs e)
        {
            if (!LicensePolicy.EnableIntegrationService)
            {
                var policyExceptionThread = new Thread(() =>
                { throw new PolicyViolationException("New license prohibited the work of the Integration Service. The application will close itself."); })
                {
                    IsBackground = true,
                    Name = "LicenseEnforcement"
                };
                policyExceptionThread.Start();

                // If we should close, no more actions are required
                return;
            }

            // Apply the new license to the internal objects
            _messageCore.UpdateLicensedFunctionality();
        }

        private void CheckClients()
        {
            try
            {
                while (true)
                {
                    // Finish clients notifications and only then interrupt the thread
                    NotifyClients(ClientNotificationType.CheckAvailability);
                    Thread.Sleep(60000);
                }
            }
            catch (ThreadInterruptedException) { }
        }

        /// <summary>
        /// Send notifications to subscribers
        /// </summary>
        /// <param name="notificationType">Type of the notification</param>
        /// <param name="notifyParameters">Notification parameters</param>
        /// <returns>Result of the callback invocation</returns>
        private object NotifyClients(ClientNotificationType notificationType, params object[] notifyParameters)
        {
            string notificationName = Enum.GetName(typeof(ClientNotificationType), notificationType);

            lock (_subscribersLock)
            {
                switch (notificationType)
                {
                    // Issue schedule changes
                    case ClientNotificationType.OnScheduleChanges:
                        {
                            break;
                        }

                    // Send operator approval request to clients 
                    case ClientNotificationType.OnTrafficScheduleChangesApproval:
                        {
                            // Check clients availability before any action
                            CheckClientsAvailability();

                            // If there is no connected client or all connected clients are not 
                            // subscribed to operator approval requests - just auto apply the message
                            if (!_subscribers.Any(client => client.OperatorApprovalSubscription))
                                return false;

                            _subscribers.ForEach(client =>
                            {
                                if (client.OperatorApprovalSubscription)
                                {
                                    // Validate parameters
                                    if ((notifyParameters != null) && (notifyParameters[0] is ScheduleApprovalMessage))
                                    {
                                        // Send request
                                        client.OnTrafficScheduleChangesApproval((ScheduleApprovalMessage)notifyParameters[0]);
                                    }
                                    else
                                    {
                                        throw new IntegrationServiceException("Invalid parameters were provided to the '" +
                                            notificationName + "' notification");
                                    }
                                }
                            });

                            return true;
                        }

                    // Send operator approval result to clients, so they will be able 
                    // to close their operator approval dialogs
                    case ClientNotificationType.OnTrafficScheduleChangesApprovalResult:
                        {
                            // Check clients availability before any action
                            CheckClientsAvailability();

                            _subscribers.ForEach(client =>
                            {
                                if (client.OperatorApprovalSubscription)
                                {
                                    // Validate parameters
                                    if ((notifyParameters != null) &&
                                        (notifyParameters[0] is Guid) && (notifyParameters[1] is ApprovalType))
                                    {
                                        // Send request
                                        client.OnTrafficScheduleChangesApprovalResult((Guid)notifyParameters[0],
                                            (ApprovalType)notifyParameters[1]);
                                    }
                                    else
                                    {
                                        throw new IntegrationServiceException("Invalid parameters were provided to the '" +
                                            notificationName + "' notification");
                                    }
                                }
                            });

                            break;
                        }

                    case ClientNotificationType.OnTrafficScheduleChangesApprovalTimeoutUpdate:
                        {
                            // Check clients availability before any action
                            CheckClientsAvailability();

                            _subscribers.ForEach(client =>
                            {
                                if (client.OperatorApprovalSubscription)
                                {
                                    // Validate parameters
                                    if ((notifyParameters != null) && (notifyParameters[0] is Guid) && (notifyParameters[1] is DateTime))
                                    {
                                        // Send request
                                        client.OnTrafficScheduleChangesApprovalTimeoutUpdate((Guid)notifyParameters[0], (DateTime)notifyParameters[1]);
                                    }
                                    else
                                    {
                                        throw new IntegrationServiceException("Invalid parameters were provided to the '" +
                                            notificationName + "' notification");
                                    }
                                }
                            });
                            break;
                        }

                    // Request for cache restore
                    case ClientNotificationType.OnCacheRestore:
                        {
                            // Check clients availability before any action
                            CheckClientsAvailability();

                            // If there is no connected client or all connected clients are not 
                            // subscribed to cache restore approval requests - just reject the cache restore
                            if (!_subscribers.Any(client => client.CacheRestoreOperatorApprovalSubscription))
                                return false;

                            _subscribers.ForEach(client =>
                            {
                                if (client.CacheRestoreOperatorApprovalSubscription)
                                {
                                    // Validate parameters
                                    if ((notifyParameters != null) &&
                                        (notifyParameters[0] is IEnumerable<string>))
                                    {
                                        // Send request
                                        client.OnCacheRestoreApproval(
                                            (IEnumerable<string>)notifyParameters[0]);
                                    }
                                    else
                                    {
                                        throw new IntegrationServiceException(
                                            "Invalid parameters were provided to the '" +
                                            notificationName + "' notification");
                                    }
                                }
                            });

                            return true;
                        }

                    // Send cache operator approval result to clients, so they will be able 
                    // to close their cache operator approval dialogs
                    case ClientNotificationType.OnCacheRestoreResult:
                        {
                            // Check clients availability before any action
                            CheckClientsAvailability();

                            _subscribers.ForEach(client =>
                            {
                                if (client.CacheRestoreOperatorApprovalSubscription)
                                {
                                    // Validate parameters
                                    if ((notifyParameters != null) && (notifyParameters[0] is ApprovalType))
                                    {
                                        // Send request
                                        client.OnCacheRestoreApprovalResult((ApprovalType)notifyParameters[0]);
                                    }
                                    else
                                    {
                                        throw new IntegrationServiceException("Invalid parameters were provided to the '" +
                                            notificationName + "' notification");
                                    }
                                }
                            });

                            break;
                        }

                    // Check availability of clients
                    case ClientNotificationType.CheckAvailability:
                        {
                            _subscribers.RemoveAll(client => !client.IsAlive);

                            _subscribers.ForEach(client => client.CheckAvailability());

                            break;
                        }
                }
            }
            return null;
        }

        private void CheckClientsAvailability()
        {
            _subscribers.ForEach(client => client.CheckClientAvailability());
            _subscribers.RemoveAll(client => !client.IsAlive);
        }

        #endregion

        #region Straight methods

        public IEnumerable<ScheduleChange> GetScheduleChanges(string server, int list, DateTime fromDate, DateTime toDate,
            ChangeDirectionType changesDirection = ChangeDirectionType.ToAutomation | ChangeDirectionType.FromAutomation)
        {
            return _messageCore.ExecuteProcessorCommand(ProcessorCommandType.GetScheduleChanges,
                server, list, fromDate, toDate, changesDirection) as IEnumerable<ScheduleChange>;
        }

        public void ScheduleChangesApprovalResult(Guid messageId, ApprovalType approvalResult)
        {
            _messageCore.ExecuteProcessorCommand(ProcessorCommandType.ScheduleChangesApprovalResult,
                messageId, approvalResult);
        }

        public void IncreaseApprovalTimeout(Guid messageId)
        {
            _messageCore.ExecuteProcessorCommand(ProcessorCommandType.IncreaseApprovalTimeout, messageId);
        }

        public void CacheRestoreApprovalResult(ApprovalType approvalResult)
        {
            _messageCore.ExecuteProcessorCommand(ProcessorCommandType.CachRestoreApprovalResult,
                approvalResult);
        }

        public void PlaylistReset(string channelName, bool initiate)
        {
            // TBD...
        }

        public IEnumerable<PlaylistResetChannelState> GetPlaylistResetChannelsState()
        {
            // TBD...
            return null;
        }

        #endregion

        #region Client notifications

        public bool RegisterScheduleChangesListener(string server, int list, ChangeDirectionType changesDirection)
        {
            try
            {
                IIntegrationServiceClient callback = _callbackCreator();

                lock (_subscribersLock)
                {
                    if (!_subscribers.Exists(client => client.IsCurrentCallback(callback)))
                        _subscribers.Add(new AsyncIntegrationServiceClient(callback));

                    _subscribers.Find(client => client.IsCurrentCallback(callback)).AddChannel(new Channel(server, list));
                }
                return true;
            }
            catch (CommunicationException ex)
            {
                ServiceLogger.Error("Error while registering ScheduleChangesListener", ex);
                return false;
            }
        }

        public void UnregisterScheduleChangesListener(string server, int list)
        {
            try
            {
                IIntegrationServiceClient callback = _callbackCreator();

                lock (_subscribersLock)
                {
                    if (_subscribers.Exists(client => client.IsCurrentCallback(callback)))
                        _subscribers.Find(client => client.IsCurrentCallback(callback)).RemoveChannel(new Channel(server, list));

                    _subscribers.RemoveAll(client => !client.HasCallbacks());
                }
            }
            catch (CommunicationException ex)
            {
                ServiceLogger.Error("Error while unregistering ScheduleChangesListener", ex);
            }
        }

        public void UnregisterAllScheduleChangesListeners(string server)
        {
            try
            {
                IIntegrationServiceClient callback = _callbackCreator();

                lock (_subscribersLock)
                {
                    if (_subscribers.Exists(client => client.IsCurrentCallback(callback)))
                        _subscribers.Find(client => client.IsCurrentCallback(callback)).RemoveAllChannels();

                    _subscribers.RemoveAll(client => !client.HasCallbacks());
                }
            }
            catch (CommunicationException ex)
            {
                ServiceLogger.Error("Error while unregistering ScheduleChangesListener", ex);
            }
        }

        public bool RegisterOperatorApprovalListener()
        {
            try
            {
                IIntegrationServiceClient callback = _callbackCreator();

                lock (_subscribersLock)
                {
                    if (!_subscribers.Exists(client => client.IsCurrentCallback(callback)))
                        _subscribers.Add(new AsyncIntegrationServiceClient(callback));

                    _subscribers.Find(client => client.IsCurrentCallback(callback)).OperatorApprovalSubscription = true;
                }
                return true;
            }
            catch (CommunicationException ex)
            {
                ServiceLogger.Error("Error while registering OperatorApprovalListener", ex);
                return false;
            }
        }

        public void UnregisterOperatorApprovalListener()
        {
            try
            {
                IIntegrationServiceClient callback = _callbackCreator();

                lock (_subscribersLock)
                {
                    if (_subscribers.Exists(client => client.IsCurrentCallback(callback)))
                        _subscribers.Find(client => client.IsCurrentCallback(callback)).OperatorApprovalSubscription = false;

                    _subscribers.RemoveAll(client => !client.HasCallbacks());
                }
            }
            catch (CommunicationException ex)
            {
                ServiceLogger.Error("Error while unregistering OperatorApprovalListener", ex);
            }
        }

        public bool RegisterCacheRestoreOperatorApprovalListener()
        {
            try
            {
                IIntegrationServiceClient callback = _callbackCreator();

                lock (_subscribersLock)
                {
                    if (!_subscribers.Exists(client => client.IsCurrentCallback(callback)))
                        _subscribers.Add(new AsyncIntegrationServiceClient(callback));

                    _subscribers.Find(client => client.IsCurrentCallback(callback)).CacheRestoreOperatorApprovalSubscription = true;
                }
                return true;
            }
            catch (CommunicationException ex)
            {
                ServiceLogger.Error("Error while registering OperatorApprovalListener", ex);
                return false;
            }
        }

        public void UnregisterCacheRestoreOperatorApprovalListener()
        {
            try
            {
                IIntegrationServiceClient callback = _callbackCreator();

                lock (_subscribersLock)
                {
                    if (_subscribers.Exists(client => client.IsCurrentCallback(callback)))
                        _subscribers.Find(client => client.IsCurrentCallback(callback)).CacheRestoreOperatorApprovalSubscription = false;

                    _subscribers.RemoveAll(client => !client.HasCallbacks());
                }
            }
            catch (CommunicationException ex)
            {
                ServiceLogger.Error("Error while unregistering OperatorApprovalListener", ex);
            }
        }

        internal object OnClientNotifyInvoke(object sender, IntegrationServiceEventArgs e)
        {
            if (e is ScheduleChangesEventArgs args)
            {
                return NotifyClients(ClientNotificationType.OnScheduleChanges,
                                     args.ServerName,
                                     args.ListNumber,
                                     args.ScheduleChanges);
            }

            if (e is TrafficScheduleChangesApprovalEventArgs eventArgs)
            {
                return NotifyClients(ClientNotificationType.OnTrafficScheduleChangesApproval, eventArgs.ScheduleChanges);
            }

            if (e is TrafficScheduleChangesApprovalResultEventArgs resultEventArgs)
            {
                return NotifyClients(ClientNotificationType.OnTrafficScheduleChangesApprovalResult,
                                     resultEventArgs.MessageId,
                                     resultEventArgs.ApprovalResult);
            }

            if (e is TrafficScheduleChangesApprovalTimeoutUpdateEventArgs timeoutEventArgs)
            {
                return NotifyClients(ClientNotificationType.OnTrafficScheduleChangesApprovalTimeoutUpdate,
                                     timeoutEventArgs.MessageId,
                                     timeoutEventArgs.RejectTimeOut);
            }

            if (e is CacheRestoreEventArgs cacheRestoreArgs)
            {
                return NotifyClients(ClientNotificationType.OnCacheRestore,
                                     cacheRestoreArgs.ChannelsWithNonEmptyAutomationList);
            }

            if (e is CacheRestoreResultEventArgs cacheRestoreResultArgs)
            {
                return NotifyClients(ClientNotificationType.OnCacheRestoreResult,
                                     cacheRestoreResultArgs.ApprovalResult);
            }

            return null;
        }

        #endregion
    }
}

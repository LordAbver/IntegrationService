using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace Services.IntegrationService
{
    /// <summary>
    /// Routes the messages from external systems to the Automation and vice versa
    /// </summary>
    public class MessageCore : IDisposable
    {
        #region Constants and variables

        private volatile bool _disposed, _workThreadStop;
        private readonly Thread _workThread;

        private readonly IIntegrationService _integrationService;
        /// <summary>
        /// Manager of the processors
        /// </summary>
        private ProcessorManager _processorManager;
        /// <summary>
        /// Manager of the external interfaces
        /// </summary>
        private ExternalInterfaceManager _externalInterfaceManager;
        /// <summary>
        /// Queue to store the sent messages. The messages will be kept for 30 minutes or 
        /// until they are acknowledged or replied. This queue will allow to implement 
        /// chunking message sending
        /// </summary>
        private ConcurrentQueue<ProcessingMessage> _sendingQueue;
        /// <summary>
        /// Maximum number of CommunicationItems to send in one chunk
        /// </summary>
        private int _chunkSize;
        /// <summary>
        /// Timeout to wait for the acknowledgement or reply (in milliseconds)
        /// </summary>
        private int _resposeTimeout;
        /// <summary>
        /// 'True' to resend the messages which response timeout is over, 
        /// 'False' to remove them from the sending queue
        /// </summary>
        private bool _resendUnrespondedMessages;

        internal event ClientNotifyEventHandler OnClientNotify;

        #endregion

        #region Class methods

        public MessageCore(MessageCoreParameters parameters)
        {
            // Obtain reference to main service class
            _integrationService = AppDomain.CurrentDomain.GetData("IntegrationService") as IIntegrationService;
            if (_integrationService == null)
                throw new MessageCoreException("Reference to main service class was not obtained");

            // Initialize parameters depending on the configuration
            _chunkSize = parameters.ChunkSize;
            _resposeTimeout = parameters.ResponseTimeout * 1000 * 60;
            _resendUnrespondedMessages = parameters.ResendUnrespondedMessages;

            // Start work thread
            _workThread = new Thread(Execute) { Name = GetType().Name };
            _workThread.Start(parameters);
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
                _workThreadStop = true;
                if (_workThread != null && _workThread.ThreadState != ThreadState.Unstarted)
                    _workThread.Join();

                if (disposing)
                {
                }
                _disposed = true;
            }
        }

        ~MessageCore()
        {
            Dispose(false);
        }

        #endregion

        #region Private working methods

        private void Execute(object parameters)
        {
            if (!(parameters is MessageCoreParameters))
                throw new MessageCoreException("Invalid parameters object was provided to work thread");

            // Create SendingQueue and load from cache
            _sendingQueue = (ConcurrentQueue<ProcessingMessage>)DataStorage.Instance.LoadCoreSendingQueue();
            if (_sendingQueue.Any() && _integrationService.ApplicationStart)
                _integrationService.CacheProcess = CacheProcessStatus.VerifyCurrentSchedule;
            ServiceLogger.Debug("SendingQueue was loaded from cache");

            // Create processors
            _processorManager = new ProcessorManager((parameters as MessageCoreParameters).ProcessorsManagerParameters);
            _processorManager.OnProcessorMessage += OnProcessorsMessage;
            _processorManager.OnClientNotify += OnClientNotifyInvoke;
            ServiceLogger.Debug("ProcessorManager was created");

            // Create external interfaces
            _externalInterfaceManager = new ExternalInterfaceManager((parameters as MessageCoreParameters).ExternalInterfacesManagerParameters);
            ServiceLogger.Debug("ExternalInterfaceManager was created");

            while (!_workThreadStop)
            {
                // Invoke ReceiveMessage of all configured interfaces
                ReceiveMessages();

                // Send messages from the sendingQueue
                ProcessSendingQueue();

                // Purge outdated messages from the sendingQueue
                PurgeSendingQueue();

                Thread.Sleep(5000);
            }

            // Stop external interfaces
            _externalInterfaceManager.Dispose();
            ServiceLogger.Debug("ExternalInterfaceManager was disposed");

            // Stop processors
            _processorManager.Dispose();
            ServiceLogger.Debug("ProcessorManager was disposed");
        }

        #region Receiving functionality

        /// <summary>
        /// Receive messages from the external interfaces
        /// </summary>
        private void ReceiveMessages()
        {
            foreach (CommunicationInterfaceType interfaceType in Enum.GetValues(typeof(CommunicationInterfaceType)))
            {
                var messageToReceive = _externalInterfaceManager.ReceiveMessage(interfaceType);
                if (messageToReceive != null)
                {
                    if (!messageToReceive.Any())
                    {
                        ServiceLogger.Warning("Unknown empty message was received");
                    }
                    else
                    {
                        // Analyze acknowledgements and replies to previously sent messages
                        if (!ProcessResposeMessage(messageToReceive))
                        {
                            // Forward the message to the processor
                            try
                            {
                                _processorManager.ExecuteProcessorCommand(ProcessorCommandType.AddProcessingMessage,
                                                                          new ProcessingMessage(messageToReceive));
                            }
                            catch (ProcessorManagerException ex)
                            {
                                // Determine the right acknowledgement for the message
                                AcknowledgementItem ack = ProcessorMessageInterfaceMatch.
                                    CreateAcknowledgementForMessageType(messageToReceive.First().MessageType);

                                ack.OriginalMessage = messageToReceive.First();
                                if (ack is BxfAcknowledgementItem)
                                {
                                    (ack as BxfAcknowledgementItem).Status = "error";
                                    (ack as BxfAcknowledgementItem).Error = "system_unavailable";
                                    (ack as BxfAcknowledgementItem).ErrorDescription = ex.Message;

                                    // Send the 'error' acknowledgement back to Traffic
                                    OnProcessorsMessage(this, new ProcessorMessageEventArgs(new[] { ack }));
                                }
                                // Add other acknowledgement types...

                                ServiceLogger.Error("MessageCore.ReceiveMessages: " + ex.Message, ex);
                            }
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Process acknowledgements and replies to the previously sent messages: 
        /// route them to the original processor if required and remove the sent 
        /// message from the sendingQueue
        /// </summary>
        /// <param name="message">Receive message</param>
        /// <returns>'True' if the message is processed in this method otherwise 'False'</returns>
        private bool ProcessResposeMessage(IEnumerable<CommunicationItem> message)
        {
            // Handle the BXF "Application Acknowledgement"
            if (message.First().MessageType == BxfMessageTypes.APPLICATION_ACKNOWLEDGEMENT)
            {
                if (message.First() is BxfAcknowledgementItem ack)
                {
                    // Find the original message
                    var sentMessage = _sendingQueue.FirstOrDefault(sentItem => sentItem.Message.First().MessageId == ack.OriginMessageId);
                    if (sentMessage != null)
                    {
                        switch (sentMessage.Message.First().MessageType)
                        {
                            case BxfMessageTypes.PLAYLIST:
                            case BxfMessageTypes.ASRUN:
                            case BxfMessageTypes.CONTENT:
                                // Add more message types...
                                {
                                    // No need to forward this acknowledgement to the original processor
                                    // So, only mark the original message with final status
                                    sentMessage.MessageStatus = ProcessingMessageStatusType.Replied;
                                    // Save _sendingQueue to DB
                                    DataStorage.Instance.SaveCoreSendingQueue(_sendingQueue);

                                    ServiceLogger.DebugFormat("Message \"{0}\" was replied", ack.OriginMessageId);

                                    break;
                                }
                                /* Commented until the "Playlist reset" functionality is implemented
                                case BxfMessageTypes.PLAYLIST_QUERY:
                                {
                                    // Forward this acknowledgement to the original processor
                                    try
                                    {
                                        _processorManager.ExecuteProcessorCommand(ProcessorCommandType.AddProcessingMessage,
                                                                                  ProcessorType.PlaylistReset, new ProcessingMessage(message));                                    
                                    }
                                    catch (ProcessorManagerException) { }

                                    // Update the status of the original message depending on the acknowledgement
                                    if (ack.Status == "invalid" || ack.Status == "error")
                                        _sendingQueue.First().MessageStatus = ProcessingMessageStatusType.Failed;
                                    else
                                        _sendingQueue.First().MessageStatus = ProcessingMessageStatusType.Acknowledged;

                                    break;
                                }
                                */
                        }

                    }
                    else
                        ServiceLogger.Warning("Received acknowledgement for unknown message");

                    return true;
                }
            }
            // Handle the "Playlist Query Reply" message

            // Handle PMCP messages

            // Handle the BXF "Branding Acknowledgement"
            if (message.First().MessageType == BrandingMessageTypes.BRANDING_ACKNOWLEDGEMENT)
            {
                if (message.First() is BrandingAcknowledgementItem ack)
                {
                    // Find the original message
                    var sentMessage = _sendingQueue.FirstOrDefault(sentItem => sentItem.Message.First().MessageId == ack.OriginMessageId);
                    if (sentMessage != null && sentMessage.Message.First().MessageType == BrandingMessageTypes.BRANDING_INFORMATION)
                    {
                        if (ack.Status == "OK")
                        {
                            // No need to forward this acknowledgement to the original processor
                            // So, only mark the original message with final status
                            sentMessage.MessageStatus = ProcessingMessageStatusType.Replied;

                            // Save _sendingQueue to DB
                            DataStorage.Instance.SaveCoreSendingQueue(_sendingQueue);

                            ServiceLogger.DebugFormat("Message \"{0}\" was replied", ack.OriginMessageId);
                        }

                        if (ack.Status == "error")
                        {
                            sentMessage.MessageStatus = ProcessingMessageStatusType.New;
                            sentMessage.SendTime = DateTime.Now;

                            // Save _sendingQueue to DB
                            DataStorage.Instance.SaveCoreSendingQueue(_sendingQueue);

                            ServiceLogger.DebugFormat("Message \"{0}\" was replied with error ({1}:{2})", ack.OriginMessageId, ack.Error, ack.ErrorDescription);
                        }
                    }
                    return true;
                }
            }

            return false;
        }

        #endregion

        #region Sending functionality

        /// <summary>
        /// Send messages from the sendingQueue to the external interfaces
        /// </summary>
        private void ProcessSendingQueue()
        {
            bool queueStateChanged = false;

            var messagesToSend = _sendingQueue.
                Where(message => message.MessageStatus == ProcessingMessageStatusType.New).
                OrderBy(message => message.SendTime).Take(5);
            foreach (var messageToSend in messagesToSend)
            {
                try
                {
                    // Transfer to external interface if it ready
                    ServiceLogger.VerboseFormat("Trying to send \"{0}\" message with id \"{1}\"",
                                                messageToSend.Message.First().MessageType, messageToSend.MessageId);
                    _externalInterfaceManager.SendMessage(messageToSend.Message);
                    messageToSend.SendTime = DateTime.Now;

                    // Set final status for acknowledgment-like messages
                    if (messageToSend.Message.First().MessageType == BxfMessageTypes.APPLICATION_ACKNOWLEDGEMENT ||
                        messageToSend.Message.First().MessageType == BrandingMessageTypes.BRANDING_ACKNOWLEDGEMENT)
                    {
                        messageToSend.MessageStatus = ProcessingMessageStatusType.Replied;
                    }
                    // Set intermediate status for other messages
                    else
                    {
                        messageToSend.MessageStatus = ProcessingMessageStatusType.Sent;
                    }

                    // Save _sendingQueue items state to DB
                    queueStateChanged = true;
                }
                catch (ExternalInterfaceManagerException ex)
                {
                    if (ex is NoCommunicationInterfaceException ||
                        ex is NotAvailableCommunicationInterfaceException)
                    {
                        // Stop sending attempts for this message
                        messageToSend.MessageStatus = ProcessingMessageStatusType.Failed;

                        // Save _sendingQueue items state to DB
                        queueStateChanged = true;

                        ServiceLogger.ErrorFormat("MessageCore.ProcessSendingQueue: {0}. Removing message from the sending queue", ex.Message);
                        break;
                    }

                    ServiceLogger.Error("MessageCore.ProcessSendingQueue: " + ex.Message, ex);
                }
            }

            // Save _sendingQueue to DB if needed
            if (queueStateChanged)
                DataStorage.Instance.SaveCoreSendingQueue(_sendingQueue);
        }

        /// <summary>
        /// Removes the old messages from the sendingQueue and 
        /// optionally put them back to sendingQueue
        /// </summary>
        private void PurgeSendingQueue()
        {
            bool queueStateChanged = false;
            ProcessingMessage messageToInspect;
            while (_sendingQueue.TryPeek(out messageToInspect))
            {
                // Remove completed and failed messages from the queue
                if (messageToInspect.MessageStatus == ProcessingMessageStatusType.Replied ||
                    messageToInspect.MessageStatus == ProcessingMessageStatusType.Failed)
                {
                    _sendingQueue.TryDequeue(out messageToInspect);
                    // Save _sendingQueue to DB
                    queueStateChanged = true;
                    continue;
                }

                // Detect messages which response timeout is over
                if (messageToInspect.SendTime.AddMilliseconds(_resposeTimeout).CompareTo(DateTime.Now) < 0)
                {
                    // Remove them from queue
                    if (_sendingQueue.TryDequeue(out messageToInspect))
                    {
                        // Save _sendingQueue to DB
                        queueStateChanged = true;

                        // Optionally add them in the queue again
                        if (_resendUnrespondedMessages)
                        {
                            // Reset response timeout
                            messageToInspect.MessageStatus = ProcessingMessageStatusType.New;
                            messageToInspect.SendTime = DateTime.Now;

                            // Put in the queue again
                            _sendingQueue.Enqueue(messageToInspect);
                        }
                        ServiceLogger.InformationalFormat("Unresponded message ({0}) was {1} the sending queue",
                            messageToInspect.Message.First().MessageId, _resendUnrespondedMessages ? "returned to" : "removed from");
                    }
                    else
                        break;
                }
                else
                    break;
            }

            // Save _sendingQueue to DB if needed
            if (queueStateChanged)
                DataStorage.Instance.SaveCoreSendingQueue(_sendingQueue);
        }

        /// <summary>
        /// This callback is invoked by the processors
        /// </summary>
        private void OnProcessorsMessage(object sender, ProcessorMessageEventArgs e)
        {
            if (e == null || e.Message == null)
            {
                ServiceLogger.Warning("There was an attempt to send a 'null' message");
                return;
            }

            bool queueStateChanged = false;
            int lastNumber = 0;
            while (lastNumber < e.Message.Count())
            {
                string tmpChunkMessageId = Guid.NewGuid().ToString("D");

                // Split into chunks
                var chunk = e.Message.Skip(lastNumber).Take(_chunkSize);
                foreach (var chunkItem in chunk)
                {
                    // Assign MessageIds
                    chunkItem.MessageId = tmpChunkMessageId;
                }

                // Store into SendingQueue
                _sendingQueue.Enqueue(new ProcessingMessage(chunk));
                // Save _sendingQueue to DB
                queueStateChanged = true;

                // Update counter
                lastNumber += chunk.Count();
            }

            // Save _sendingQueue to DB if needed
            if (queueStateChanged)
                DataStorage.Instance.SaveCoreSendingQueue(_sendingQueue);
        }

        #endregion

        #endregion

        #region Internal methods

        /// <summary>
        /// Apply the changed configuration
        /// </summary>
        internal void UpdateConfiguration(Dictionary<string, ConfigurationPropertyNode> changedProperties)
        {
            ConfigurationPropertyNode configurationNode;

            if (changedProperties.TryGetValue("ChunkSize", out configurationNode))
                _chunkSize = (ushort)configurationNode.NewValue;

            if (changedProperties.TryGetValue("ResponseTimeout", out configurationNode))
                _resposeTimeout = (ushort)configurationNode.NewValue * 1000 * 60;

            if (changedProperties.TryGetValue("ResendUnrespondedMessages", out configurationNode))
                _resendUnrespondedMessages = (bool)configurationNode.NewValue;

            if (changedProperties.TryGetValue("ProcessorsManagerParameters", out configurationNode))
                _processorManager.UpdateConfiguration(configurationNode.Children);

            if (changedProperties.TryGetValue("ExternalInterfacesManagerParameters", out configurationNode))
                _externalInterfaceManager.UpdateConfiguration(configurationNode.Children);
        }

        /// <summary>
        /// Apply the changed license parameters to the license-enabled objects
        /// </summary>
        internal void UpdateLicensedFunctionality()
        {
            _processorManager.UpdateLicensedFunctionality();
            _externalInterfaceManager.UpdateLicensedFunctionality();
        }

        /// <summary>
        /// Execute a specific command on a processor
        /// </summary>
        /// <param name="commandType">Type of the command to a processor</param>
        /// <param name="commandParameters">Parameters of the command</param>
        /// <returns>Result of the command execution</returns>
        internal object ExecuteProcessorCommand(ProcessorCommandType commandType, params object[] commandParameters)
        {
            return _processorManager.ExecuteProcessorCommand(commandType, commandParameters);
        }

        /// <summary>
        /// Callback from processors
        /// </summary>
        internal object OnClientNotifyInvoke(object sender, IntegrationServiceEventArgs e)
        {
            if (OnClientNotify != null)
                return OnClientNotify(sender, e);
            return null;
        }

        #endregion
    }
}

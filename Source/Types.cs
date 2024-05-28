using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.ServiceModel;
using System.Threading;

namespace Services.IntegrationService
{
    #region Async callback classes

    internal sealed class AsyncIntegrationServiceClient : IDisposable, IIntegrationServiceClient
    {
        #region Variables

        private Thread _callbackThread;
        private volatile bool _isAlive;
        private bool _isDisposed;

        private ConcurrentQueue<IntegrationServiceCallbackInfo> _callbackEvents;
        private List<Channel> _channels;
        private object _channelsLocker;
        private volatile bool _availabilityCheck;
        private readonly EventWaitHandle _handle;
        private IIntegrationServiceClient _callback;

        #endregion

        #region Class methods

        public AsyncIntegrationServiceClient(IIntegrationServiceClient callback)
        {
            _callback = callback;
            IsAlive = true;
            _handle = new ManualResetEvent(false);
            _callbackEvents = new ConcurrentQueue<IntegrationServiceCallbackInfo>();
            _channels = new List<Channel>();
            _channelsLocker = new object();

            Start();
        }

        ~AsyncIntegrationServiceClient()
        {
            Dispose(false);
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        private void Dispose(bool disposing)
        {
            if (!_isDisposed)
            {
                // Free the unmanaged resources
                Stop();
                _callbackThread = null;

                if (disposing)
                {
                    // Free the managed resources
                    _handle.Dispose();
                    _callback = null;
                    _callbackEvents = null;
                    _channels.Clear();
                    _channels = null;
                    _channelsLocker = null;

                    GC.Collect();
                    GC.SuppressFinalize(this);
                }

                _isDisposed = true;
            }
        }

        #endregion

        #region Public Properties

        public bool IsAlive
        {
            get { return _isAlive; }
            private set { _isAlive = value; }
        }

        public bool OperatorApprovalSubscription { get; set; }

        public bool CacheRestoreOperatorApprovalSubscription { get; set; }

        #endregion

        #region Private and internal working methods

        private void Start()
        {
            if (_callbackThread != null)
            {
                if (_callbackThread.ThreadState != ThreadState.Running
                    && _callbackThread.ThreadState != ThreadState.WaitSleepJoin)
                {
                    _callbackThread = new Thread(ExecuteCallbacks) { Name = "IntegrationServiceCallbackThread" };
                    _callbackThread.Start();
                }
            }
            else
            {
                _callbackThread = new Thread(ExecuteCallbacks) { Name = "IntegrationServiceCallbackThread" };
                _callbackThread.Start();
            }
        }

        private void Stop()
        {
            if (_callbackThread != null)
                if (_callbackThread.ThreadState == ThreadState.Running
                    || _callbackThread.ThreadState == ThreadState.WaitSleepJoin)
                {
                    IsAlive = false;
                    _callbackThread.Interrupt();
                    _callbackThread.Join();
                }
        }

        private void ExecuteCallbacks()
        {
            try
            {
                while (IsAlive)
                {
                    _handle.WaitOne();
                    _handle.Reset();
                    while (!_callbackEvents.IsEmpty)
                    {
                        IntegrationServiceCallbackInfo info;
                        _callbackEvents.TryDequeue(out info);
                        info.ExecuteCallback(_callback);
                    }
                    if (_availabilityCheck)
                    {
                        _callback.CheckAvailability();
                        _availabilityCheck = false;
                    }
                }
            }
            catch (CommunicationException ex)
            {
                ServiceLogger.Error("Client Disconnected in case of CommunicationException", ex);
                IsAlive = false;
            }
            catch (TimeoutException ex)
            {
                ServiceLogger.Error("Client Disconnected in case of TimeoutException", ex);
                IsAlive = false;
            }
            catch (ObjectDisposedException ex)
            {
                ServiceLogger.Error("Client Disconnected in case of ObjectDisposedException", ex);
                IsAlive = false;
            }
            catch (ThreadInterruptedException)
            {
            }
        }

        internal void AddChannel(Channel channel)
        {
            lock (_channelsLocker)
            {
                if (!_channels.Contains(channel))
                {
                    _channels.Add(channel);
                }
            }
        }

        internal void RemoveChannel(Channel channel)
        {
            lock (_channelsLocker)
            {
                _channels.Remove(channel);
            }
        }

        internal void RemoveAllChannels()
        {
            lock (_channelsLocker)
            {
                _channels.Clear();
            }
        }

        internal bool HasCallbacks()
        {
            lock (_channelsLocker)
            {
                return (_channels.Count > 0) && OperatorApprovalSubscription;
            }
        }

        internal bool IsCurrentCallback(IIntegrationServiceClient callback)
        {
            return callback.Equals(_callback);
        }

        #endregion

        #region Public working methods

        public void OnScheduleChanges(string server, int list, IEnumerable<ScheduleChange> scheduleChanges)
        {
            lock (_channelsLocker)
            {
                var channel = new Channel(server, list);
                if (_channels.Contains(channel))
                {
                    _callbackEvents.Enqueue(new ScheduleChangesCallbackInfo(channel, scheduleChanges));
                }
            }
            _handle.Set();
        }

        public void OnTrafficScheduleChangesApproval(ScheduleApprovalMessage scheduleChanges)
        {
            if (OperatorApprovalSubscription)
            {
                _callbackEvents.Enqueue(new TrafficScheduleChangesApprovalCallbackInfo(scheduleChanges));
                _handle.Set();
            }
        }

        public void OnTrafficScheduleChangesApprovalResult(Guid messageId, ApprovalType approvalResult)
        {
            if (OperatorApprovalSubscription)
            {
                _callbackEvents.Enqueue(new TrafficScheduleChangesApprovalResultCallbackInfo(messageId,
                                                                                             approvalResult));
                _handle.Set();
            }
        }

        public void OnTrafficScheduleChangesApprovalTimeoutUpdate(Guid messageId, DateTime newAutoRejectTime)
        {
            if (OperatorApprovalSubscription)
            {
                _callbackEvents.Enqueue(new TrafficScheduleChangesApprovalTimeoutUpdateCallbackInfo(messageId,
                                                                                                    newAutoRejectTime));
                _handle.Set();
            }
        }

        public void OnCacheRestoreApproval(IEnumerable<string> channelsWithNonEmptyAutomationList)
        {
            if (CacheRestoreOperatorApprovalSubscription)
            {
                _callbackEvents.Enqueue(new CacheRestoreApprovalCallbackInfo(channelsWithNonEmptyAutomationList));
                _handle.Set();
            }
        }

        public void OnCacheRestoreApprovalResult(ApprovalType approvalResult)
        {
            if (CacheRestoreOperatorApprovalSubscription)
            {
                _callbackEvents.Enqueue(new CacheRestoreApprovalResultCallbackInfo(approvalResult));
                _handle.Set();
            }
        }

        public void OnPlaylistResetChannelStateChanged(PlaylistResetChannelState state)
        {
            // TBD...
        }

        /// <summary>
        /// Check client availability in the callbacks thread
        /// </summary>
        public void CheckAvailability()
        {
            _availabilityCheck = true;
            _handle.Set();
        }

        /// <summary>
        /// Check client availability in the current thread
        /// </summary>
        public void CheckClientAvailability()
        {
            try
            {
                _callback.CheckAvailability();
            }
            catch (CommunicationException ex)
            {
                ServiceLogger.Error("Client Disconnected in case of CommunicationException", ex);
                IsAlive = false;
            }
            catch (TimeoutException ex)
            {
                ServiceLogger.Error("Client Disconnected in case of TimeoutException", ex);
                IsAlive = false;
            }
            catch (ObjectDisposedException ex)
            {
                ServiceLogger.Error("Client Disconnected in case of ObjectDisposedException", ex);
                IsAlive = false;
            }
        }

        #endregion

        #region Standard methods and overrides

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            return (obj is AsyncIntegrationServiceClient) && Equals((AsyncIntegrationServiceClient)obj);
        }

        public bool Equals(AsyncIntegrationServiceClient other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return other._channels.SequenceEqual(_channels) &&
                   (other.OperatorApprovalSubscription == OperatorApprovalSubscription) &&
                   Equals(other._callback, _callback);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                int result = _channels != null ? _channels.GetHashCode() : 0;
                result = (result * 397) ^ (_callback != null ? _callback.GetHashCode() : 0);
                return result;
            }
        }

        public static bool operator ==(AsyncIntegrationServiceClient left, AsyncIntegrationServiceClient right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(AsyncIntegrationServiceClient left, AsyncIntegrationServiceClient right)
        {
            return !Equals(left, right);
        }

        #endregion
    }

    #region CallbackInfo classes

    internal abstract class IntegrationServiceCallbackInfo
    {
        public abstract void ExecuteCallback(IIntegrationServiceClient client);
    }

    internal class ScheduleChangesCallbackInfo : IntegrationServiceCallbackInfo
    {
        public Channel MonitoredChannel { get; private set; }
        public IEnumerable<ScheduleChange> ScheduleChanges { get; private set; }


        public ScheduleChangesCallbackInfo(Channel channel, IEnumerable<ScheduleChange> scheduleChanges)
        {
            MonitoredChannel = channel;
            ScheduleChanges = scheduleChanges;
        }

        public override void ExecuteCallback(IIntegrationServiceClient client)
        {
            client.OnScheduleChanges(MonitoredChannel.Server, MonitoredChannel.List, ScheduleChanges);
        }
    }

    internal class TrafficScheduleChangesApprovalCallbackInfo : IntegrationServiceCallbackInfo
    {
        public ScheduleApprovalMessage ScheduleChanges { get; private set; }


        public TrafficScheduleChangesApprovalCallbackInfo(ScheduleApprovalMessage scheduleChanges)
        {
            ScheduleChanges = scheduleChanges;
        }

        public override void ExecuteCallback(IIntegrationServiceClient client)
        {
            client.OnTrafficScheduleChangesApproval(ScheduleChanges);
        }
    }

    internal class TrafficScheduleChangesApprovalResultCallbackInfo : IntegrationServiceCallbackInfo
    {
        public Guid MessageId { get; private set; }
        public ApprovalType ApprovalResult { get; private set; }


        public TrafficScheduleChangesApprovalResultCallbackInfo(Guid messageId, ApprovalType approvalResult)
        {
            MessageId = messageId;
            ApprovalResult = approvalResult;
        }

        public override void ExecuteCallback(IIntegrationServiceClient client)
        {
            client.OnTrafficScheduleChangesApprovalResult(MessageId, ApprovalResult);
        }
    }

    internal class TrafficScheduleChangesApprovalTimeoutUpdateCallbackInfo : IntegrationServiceCallbackInfo
    {
        public Guid MessageId { get; private set; }
        public DateTime RejectTimeOut { get; private set; }

        public TrafficScheduleChangesApprovalTimeoutUpdateCallbackInfo(Guid messageId, DateTime rejectTimeOut)
        {
            MessageId = messageId;
            RejectTimeOut = rejectTimeOut;
        }

        public override void ExecuteCallback(IIntegrationServiceClient client)
        {
            client.OnTrafficScheduleChangesApprovalTimeoutUpdate(MessageId, RejectTimeOut);
        }
    }

    internal class CacheRestoreApprovalCallbackInfo : IntegrationServiceCallbackInfo
    {
        public IEnumerable<string> ChannelsWithNonEmptyAutomationList { get; private set; }


        public CacheRestoreApprovalCallbackInfo(IEnumerable<string> channelsWithNonEmptyAutomationList)
        {
            ChannelsWithNonEmptyAutomationList = channelsWithNonEmptyAutomationList;
        }

        public override void ExecuteCallback(IIntegrationServiceClient client)
        {
            client.OnCacheRestoreApproval(ChannelsWithNonEmptyAutomationList);
        }
    }

    internal class CacheRestoreApprovalResultCallbackInfo : IntegrationServiceCallbackInfo
    {
        public ApprovalType ApprovalResult { get; private set; }


        public CacheRestoreApprovalResultCallbackInfo(ApprovalType approvalResult)
        {
            ApprovalResult = approvalResult;
        }

        public override void ExecuteCallback(IIntegrationServiceClient client)
        {
            client.OnCacheRestoreApprovalResult(ApprovalResult);
        }
    }

    #endregion

    #endregion

    #region Internal Notification

    internal class InternalNotifier : IInternalNotifier
    {
        private Dictionary<InternalNotificationType, List<IInternalNotificationSubscriber>> _subscribers;

        public InternalNotifier()
        {
            _subscribers = new Dictionary<InternalNotificationType, List<IInternalNotificationSubscriber>>();

        }

        public void SendNotification(InternalNotificationType notificationType, object notificationData)
        {
            List<IInternalNotificationSubscriber> subscribers;

            if (_subscribers.TryGetValue(notificationType, out subscribers))
            {
                subscribers.ForEach(subscriber => subscriber.OnInnerNotification(notificationType, notificationData));
            }
        }

        public void Subscribe(InternalNotificationType notificationType, IInternalNotificationSubscriber subscriber)
        {
            List<IInternalNotificationSubscriber> subscribers;

            if (!_subscribers.TryGetValue(notificationType, out subscribers))
            {
                subscribers = new List<IInternalNotificationSubscriber>();
                _subscribers.Add(notificationType, subscribers);
            }

            if (!subscribers.Contains(subscriber))
            {
                subscribers.Add(subscriber);
            }
        }

        public void Unsubscribe(InternalNotificationType notificationType, IInternalNotificationSubscriber subscriber)
        {
            List<IInternalNotificationSubscriber> subscribers;

            if (_subscribers.TryGetValue(notificationType, out subscribers) && subscribers.Contains(subscriber))
            {
                subscribers.Remove(subscriber);
            }
        }
    }

    #endregion

    #region Exceptions

    /// <summary>
    /// Exception thrown by the IntegrationService class methods
    /// </summary>
    [Serializable]
    public class IntegrationServiceException : Exception
    {
        public IntegrationServiceException() { }
        public IntegrationServiceException(string message) : base(message) { }
        public IntegrationServiceException(string message, Exception inner) : base(message, inner) { }
        protected IntegrationServiceException(System.Runtime.Serialization.SerializationInfo info,
                                              System.Runtime.Serialization.StreamingContext context)
            : base(info, context) { }
    }

    #endregion
}

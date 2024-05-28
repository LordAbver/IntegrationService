//
//

using System;
using System.Collections.Generic;
using System.ServiceModel;


namespace Harris.Automation.ADC.Services.IntegrationService
{
    /// <summary>
    /// The interface of Integration Service
    /// </summary>
    [ServiceContract(CallbackContract = typeof(IIntegrationServiceClient))]
    public interface IIntegrationService
    {
        /// <summary>
        /// Request schedule changes
        /// </summary>
        /// <remarks>
        /// It is assumed that this method is invoked during the initial start of the client 
        /// to display the changes made from some time in the past till the client start time
        /// </remarks>
        /// <param name="server">Device Server name</param>
        /// <param name="list">List number on the specified Device Server</param>
        /// <param name="fromDate">Start of the request period</param>
        /// <param name="toDate">End of the request period</param>
        /// <param name="changesDirection">Schedule changes came from Traffic, 
        /// made in the Automation or both ones. Default is both - 
        /// ChangeDirectionType.ToAutomation | ChangeDirectionType.FromAutomation</param>
        /// <returns>List of requested changes</returns>
        [OperationContract]
        IEnumerable<ScheduleChange> GetScheduleChanges(String server, Int32 list,
            DateTime fromDate, DateTime toDate, ChangeDirectionType changesDirection);

        /// <summary>
        /// Set the approval status of Traffic message
        /// </summary>
        /// <param name="messageId">Id of the Traffic message</param>
        /// <param name="approvalResult">Signalize that the schedule changes 
        /// from the message were applied or rejected</param>
        [OperationContract]
        void ScheduleChangesApprovalResult(Guid messageId, ApprovalType approvalResult);
        
        /// <summary>
        /// Subscribe to  the Traffic schedule changes, the client will not 
        /// be able to receive Operator Approval requests
        /// </summary>
        /// <param name="server">Device Server name</param>
        /// <param name="list">List number on the specified Device Server</param>
        /// <param name="changesDirection">Schedule changes came from Traffic, 
        /// made in the Automation or both ones. Default is both - 
        /// ChangeDirectionType.ToAutomation | ChangeDirectionType.FromAutomation</param>
        /// <returns>'True' if the subscription succeeded, 
        /// 'False' if the subscription failed</returns>
        [OperationContract]
        Boolean RegisterScheduleChangesListener(String server, Int32 list, ChangeDirectionType changesDirection);

        /// <summary>
        /// Unsubscribe from the Traffic schedule changes for specific channel
        /// </summary>
        /// <param name="server">Device Server name</param>
        /// <param name="list">List number on the specified Device Server</param>
        [OperationContract(IsOneWay = true)]
        void UnregisterScheduleChangesListener(String server, Int32 list);

        /// <summary>
        /// Unsubscribe from the Traffic schedule changes for all channels 
        /// hosted by the specified Device Server
        /// </summary>
        /// <param name="server">Device Server name</param>
        [OperationContract(IsOneWay = true)]
        void UnregisterAllScheduleChangesListeners(String server);

        /// <summary>
        /// Subscribe to the Traffic schedule changes approvals, the client 
        /// will be able to control the changes came from Traffic
        /// </summary>
        /// <returns>'True' if the subscription succeeded, 
        /// 'False' if the subscription failed</returns>
        [OperationContract]
        Boolean RegisterOperatorApprovalListener();

        /// <summary>
        /// Unsubscribe from the Traffic schedule changes approvals
        /// </summary>
        [OperationContract(IsOneWay = true)]
        void UnregisterOperatorApprovalListener();
    }

    /// <summary>
    /// The interface of Integration Service callbacks
    /// </summary>
    public interface IIntegrationServiceClient
    {
        /// <summary>
        /// Callback providing the schedule changes to subscribers
        /// </summary>
        /// <param name="server">Device Server name</param>
        /// <param name="list">List number on the specified Device Server</param>
        /// <param name="scheduleChanges">Changes happened with the schedule</param>
        [OperationContract(IsOneWay = true)]
        void OnScheduleChanges(String server, Int32 list, IEnumerable<ScheduleChange> scheduleChanges);

        /// <summary>
        /// Callback providing the schedule changes operator approval requests
        /// </summary>
        /// <param name="scheduleChanges">Schedule changes came from Traffic for operator approval</param>
        [OperationContract(IsOneWay = true)]
        void OnTrafficScheduleChangesApproval(ScheduleApprovalMessage scheduleChanges);

        /// <summary>
        /// Callback signalizes that schedule changes were handled by other Automation client.
        /// </summary>
        /// <remarks>
        /// The Operator Approval dialog in the current client should be closed
        /// </remarks>
        /// <param name="messageId">Id of the Traffic message</param>
        /// <param name="approvalResult">Schedule changes were applied or rejected</param>
        [OperationContract(IsOneWay = true)]
        void OnTrafficScheduleChangesApprovalResult(Guid messageId, ApprovalType approvalResult);

        /// <summary>
        /// Check availability of a client
        /// </summary>
        [OperationContract(IsOneWay = true)]
        void CheckAvailability();
    }
}

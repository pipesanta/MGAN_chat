import gql from "graphql-tag";

export const getAllMessages = gql`
query getAllMessages{
  getMessages
}
`;

export const sendMessage = gql`
mutation sendMessage($msg: String!) {
    sendMessage(msg: $msg) 
}
`;

export const onMessageArriveSubscription = gql `
subscription{
  onNewMsgArrived
}`;


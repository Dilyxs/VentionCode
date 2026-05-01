package ventionroom

import "time"

type UserDisconnection struct {
	ID UserID
}
type CheckIfUserAllowedToJoin struct {
	ID         UserID
	OutputChan chan RoomCommandResponse
}
type RoomCommandResponse struct {
	Err     error
	content string
}
type AddPlayerCommand struct {
	ID         UserID
	OutputChan chan RoomCommandResponse
}
type AddPlayerToWebsocketCommand struct {
	ID         UserID
	OutputChan chan RoomCommandResponse
}

func (mes AddPlayerToWebsocketCommand) get_sendback_chan() chan RoomCommandResponse {
	return mes.OutputChan
}

func (mes AddPlayerCommand) get_sendback_chan() chan RoomCommandResponse {
	return mes.OutputChan
}

func (mes CheckIfUserAllowedToJoin) get_sendback_chan() chan RoomCommandResponse {
	return mes.OutputChan
}

type (
	Message         interface{}
	ExternalMessage interface {
		get_sendback_chan() chan RoomCommandResponse
	}
	Room struct {
		IsClosed                 bool
		AllowedPlayers           map[UserID]bool
		PreviousMessages         []Message
		ExternalIncomingMessages chan ExternalMessage
		InternalIncomingMessages chan Message
		SocketsConn              map[UserID]chan Message // NOTE: this would have actually been *Conn
	}
)

type (
	UserID   int32
	Question struct {
		QuestionID int32
		Question   string
		Answer     string
	}
)

type QuestionAcknowledgmentResponse struct {
	Registered bool
	ID         int32
}
type UserResponse struct {
	UserID       int32
	QuestionID   int32
	Answer       string
	ResponseChan chan QuestionAcknowledgmentResponse
}
type Manager struct {
	QuestionCount             int32
	DelayBetweenEachQuestion  time.Duration
	Questions                 []Question
	ConnectionToWebsocketChan chan MessageToWebsocket
}
type MessageToWebsocket interface{}

type QuestionAnsweredResults struct {
	CorrectUsers   []UserID
	IncorrectUsers []UserID
}

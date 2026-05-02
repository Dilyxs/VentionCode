package ventionroom

import "time"

type RoomErrorCode int

const (
	RoomErrorUnknown RoomErrorCode = iota
	RoomErrorRoomClosed
	RoomErrorPermissionDenied
	RoomErrorInvalidUserID
)

type RoomError struct {
	Code RoomErrorCode
}

func (e RoomError) Error() string {
	switch e.Code {
	case RoomErrorRoomClosed:
		return "room is currently full"
	case RoomErrorPermissionDenied:
		return "don't have permission to join"
	case RoomErrorInvalidUserID:
		return "invalid user id"
	default:
		return "unknown room error"
	}
}

func NewRoomError(code RoomErrorCode) error {
	return RoomError{Code: code}
}

type RoomCommandContentCode int

const (
	RoomCommandContentNone RoomCommandContentCode = iota
	RoomCommandContentAllowedToJoin
	RoomCommandContentNoPermissionToJoin
	RoomCommandContentPermissionToJoinGame
	RoomCommandContentAddedWebsocket
)

func (c RoomCommandContentCode) String() string {
	switch c {
	case RoomCommandContentAllowedToJoin:
		return "allowed to join!"
	case RoomCommandContentNoPermissionToJoin:
		return "don't have the permission to join!"
	case RoomCommandContentPermissionToJoinGame:
		return "permission to join game!"
	case RoomCommandContentAddedWebsocket:
		return "added websocket!"
	default:
		return ""
	}
}

type UserDisconnection struct {
	ID UserID
}
type CheckIfUserAllowedToJoin struct {
	ID         UserID
	OutputChan chan RoomCommandResponse
}
type RoomCommandResponse struct {
	Err     error
	content RoomCommandContentCode
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

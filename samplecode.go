package ventionroom

import (
	"context"
	"math"
	"math/rand"
	"time"
)

func dropMessageOff(cmd Message, chans map[UserID]chan Message) {
	for _, socketChan := range chans {
		select {
		case socketChan <- cmd:
		default:
		}
	}
}

func (r *Room) Run(ctx context.Context) {
	for {
		select {
		// NOTE: cleanup everything
		case <-ctx.Done():
			return
		// NOTE: This is the internal room Request
		case new_message := <-r.InternalIncomingMessages:
			r.PreviousMessages = append(r.PreviousMessages, new_message)
			switch cmd := new_message.(type) {
			case UserDisconnection:
				delete(r.SocketsConn, cmd.ID)
			default:
				dropMessageOff(cmd, r.SocketsConn)
			}

		// NOTE: this is the request coming from outside, http request most likely
		case command := <-r.ExternalIncomingMessages:
			switch cmd := command.(type) {
			case CheckIfUserAllowedToJoin:
				if r.IsClosed {
					cmd.get_sendback_chan() <- RoomCommandResponse{Err: NewRoomError(RoomErrorRoomClosed), content: RoomCommandContentNone}
					continue
				}
				if r.AllowedPlayers[cmd.ID] {
					cmd.get_sendback_chan() <- RoomCommandResponse{content: RoomCommandContentAllowedToJoin, Err: nil}
				} else {
					cmd.get_sendback_chan() <- RoomCommandResponse{content: RoomCommandContentNoPermissionToJoin, Err: NewRoomError(RoomErrorPermissionDenied)}
				}
			case AddPlayerCommand:
				r.AllowedPlayers[cmd.ID] = true
				cmd.get_sendback_chan() <- RoomCommandResponse{content: RoomCommandContentPermissionToJoinGame, Err: nil}
			case AddPlayerToWebsocketCommand:
				localChan := make(chan Message, 1000)
				// go ReadFromWebsocket(cmd.Conn, r.SocketsConn, cmd.ID, ctx)
				// go WriteToWebsocket(cmd.Conn, localChan, ctx)
				r.SocketsConn[cmd.ID] = localChan
				// go WritePreviousMessagesToWebsocket(localChan, r.AllPreiousMessages, ctx)
				cmd.get_sendback_chan() <- RoomCommandResponse{content: RoomCommandContentAddedWebsocket, Err: nil}

			}
		}
	}
}

// AskQuestions's role is to send to ConnectionToWebsocketChan Questions, and evaluate responses that players get
func (q *Manager) AskQuestions(ctx context.Context, localChan <-chan UserResponse) {
	for range q.QuestionCount {
		questionNum := rand.Int31n(int32(len(q.Questions)))
		pickedQuestion := q.Questions[questionNum]

		// NOTE: here we send to the process that owns all the connections the question
		q.ConnectionToWebsocketChan <- Question(pickedQuestion)
		result := make(map[bool][]UserID)
		// NOTE: now we evalute the response that come through
	InfiniteLoop:
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(q.DelayBetweenEachQuestion):
				break InfiniteLoop
			// NOTE: localChan is where the responses are being propagated from
			case req := <-localChan:
				if req.QuestionID != pickedQuestion.QuestionID {
					select {
					case req.ResponseChan <- QuestionAcknowledgmentResponse{ID: rand.Int31n(math.MaxInt32), Registered: false}:
					default:
					}
					continue
				}
				if pickedQuestion.Answer == req.Answer {
					result[true] = append(result[true], UserID(req.UserID))
				} else {
					result[false] = append(result[false], UserID(req.UserID))
				}
				select {
				case req.ResponseChan <- QuestionAcknowledgmentResponse{ID: rand.Int31n(math.MaxInt32), Registered: true}:
				default:
				}
			}
		}
		// NOTE: now we send back to the ConnectionToWebsocketChan the answer that have come through
		q.ConnectionToWebsocketChan <- QuestionAnsweredResults{
			CorrectUsers:   result[true],
			IncorrectUsers: result[false],
		}
	}
}

package ventionroom

import (
	"context"
	"slices"
	"testing"
	"time"
)

var defaultWaitTime = 150 * time.Millisecond

func buildRoom(t *testing.T, isRoomClosed bool) *Room {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	room := &Room{
		IsClosed:                 isRoomClosed,
		AllowedPlayers:           make(map[UserID]bool),
		PreviousMessages:         make([]Message, 0, 10),
		SocketsConn:              make(map[UserID]chan Message),
		InternalIncomingMessages: make(chan Message, 100),
		ExternalIncomingMessages: make(chan ExternalMessage, 100),
	}
	go room.Run(ctx)
	t.Cleanup(cancel)
	return room
}

func buildManager(t *testing.T, questionCount int32, delayBetweenQuestions time.Duration) (*Manager, chan UserResponse) {
	t.Helper()
	manager := &Manager{
		QuestionCount:             questionCount,
		DelayBetweenEachQuestion:  delayBetweenQuestions,
		ConnectionToWebsocketChan: make(chan MessageToWebsocket, 100),
		Questions: []Question{
			{QuestionID: 1, Question: "What is 2+2?", Answer: "4"},
			{QuestionID: 2, Question: "What is the capital of France?", Answer: "Paris"},
			{QuestionID: 3, Question: "What color is the sky?", Answer: "Blue"},
			{QuestionID: 4, Question: "What is the largest mammal?", Answer: "Blue Whale"},
			{QuestionID: 5, Question: "What is the boiling point of water?", Answer: "100"},
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	userResponseChan := make(chan UserResponse, 100)
	go manager.AskQuestions(ctx, userResponseChan)
	t.Cleanup(cancel)

	return manager, userResponseChan
}

func assrtQuestionToWebsocket(expectedQuestions Question, readingChan <-chan MessageToWebsocket, t *testing.T) Question {
	t.Helper()
	for {
		select {
		case <-time.After(2 * defaultWaitTime):
			var zero Question
			t.Fatalf("ExpectedQuestionToWebsocket did not arrive within delay: %v", defaultWaitTime)
			return zero
		case got := <-readingChan:
			// NOTE: could be either a Question OR a RoomCommandResponse, but for this function we only care about Question, so we will just ignore the RoomCommandResponse
			if question, ok := got.(Question); ok {
				if question != expectedQuestions {
					t.Fatalf("ExpectedQuestionToWebsocket(%v) want:%v", question, expectedQuestions)
				}
				return question
			} else {
				continue
			}
		}
	}
}

func assertQuestionAcknowledgmentResponse(expectedResponse QuestionAcknowledgmentResponse, readingChan <-chan QuestionAcknowledgmentResponse, t *testing.T) QuestionAcknowledgmentResponse {
	t.Helper()
	select {
	case <-time.After(defaultWaitTime):
		var zero QuestionAcknowledgmentResponse
		t.Fatalf("ExpectedQuestionAcknowledgmentResponse did not arrive within delay: %v", defaultWaitTime)
		return zero
	case got := <-readingChan:
		if got != expectedResponse {
			t.Fatalf("ExpectedQuestionAcknowledgmentResponse(%v) want:%v", got, expectedResponse)
		}
		return got
	}
}

func assertQuestionAggreatatedResults(expectedResult QuestionAnsweredResults, readingChan <-chan MessageToWebsocket, t *testing.T) QuestionAnsweredResults {
	t.Helper()
	for {
		select {
		case <-time.After(2 * defaultWaitTime):
			var zero QuestionAnsweredResults
			t.Fatalf("ExpectedQuestionAggreatatedResults did not arrive within delay: %v", defaultWaitTime)
			return zero
		case got := <-readingChan:
			// NOTE: could be either a QuestionAnsweredResults OR a RoomCommandResponse, but for this function we only care about QuestionAnsweredResults, so we will just ignore the RoomCommandResponse
			if result, ok := got.(QuestionAnsweredResults); ok {
				if slices.Compare(result.CorrectUsers, expectedResult.CorrectUsers) != 0 || slices.Compare(result.IncorrectUsers, expectedResult.IncorrectUsers) != 0 {
					t.Fatalf("ExpectedQuestionAggreatatedResults(%v) want:%v", result, expectedResult)
				}
				return result
			} else {
				continue
			}
		}
	}
}

func assertRoomCommandResponse(expectedResponse RoomCommandResponse, readingChan <-chan RoomCommandResponse, t *testing.T) RoomCommandResponse {
	t.Helper()
	select {
	case <-time.After(defaultWaitTime):
		var emptyResponse RoomCommandResponse
		t.Fatalf("ExptedRoomCommandResponse did not arrive within delay: %v", defaultWaitTime)
		return emptyResponse
	case got := <-readingChan:
		if got != expectedResponse {
			t.Fatalf("ExptedRoomCommandResponse(%v) want:%v", got, expectedResponse)
		}
		return got
	}
}

func assertRoomNewMessage(expectedMessage Message, readingChan <-chan Message, t *testing.T) Message {
	t.Helper()
	select {
	case <-time.After(defaultWaitTime):
		var emptyMessage Message
		t.Fatalf("ExpectedMessage did not arrive within delay: %v", defaultWaitTime)
		return emptyMessage
	case got := <-readingChan:
		if got != expectedMessage {
			t.Fatalf("ExpectedMessage(%v) want:%v", got, expectedMessage)
		}
		return got
	}
}

func assertNoNewRoomMessage(readingChan <-chan Message, t *testing.T) {
	t.Helper()
	select {
	case <-time.After(3 * defaultWaitTime):
		return
	case got := <-readingChan:
		t.Fatalf("Expected no new message, but got: %v", got)
	}
}

// NOTE: for this test, we are not testing gonna build the room raw to test the cancel feature
func TestRoom_RunExitsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	room := &Room{
		IsClosed:                 false,
		AllowedPlayers:           make(map[UserID]bool),
		PreviousMessages:         make([]Message, 0, 10),
		SocketsConn:              make(map[UserID]chan Message),
		InternalIncomingMessages: make(chan Message, 100),
		ExternalIncomingMessages: make(chan ExternalMessage, 100),
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		room.Run(ctx)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(defaultWaitTime):
		t.Fatalf("Room.Run did not exit within delay: %v", defaultWaitTime)
	}
}

func TestRoom_CheckAddPlayerCommand(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name             string
		userID           UserID
		expectedResponse RoomCommandResponse
	}{
		{
			name:             "valid user ID",
			userID:           1,
			expectedResponse: RoomCommandResponse{content: RoomCommandContentPermissionToJoinGame, Err: nil},
		}, {
			name:             "invalid user ID",
			userID:           -1,
			expectedResponse: RoomCommandResponse{content: RoomCommandContentNoPermissionToJoin, Err: NewRoomError(RoomErrorInvalidUserID)},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			room := buildRoom(t, false)
			responseChan := make(chan RoomCommandResponse, 1)
			addPlayerCommand := AddPlayerCommand{ID: tc.userID, OutputChan: responseChan}
			room.ExternalIncomingMessages <- addPlayerCommand
			assertRoomCommandResponse(tc.expectedResponse, responseChan, t)
		})
	}
}

func TestRoom_CheckIfUserAllowedToJoin(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name             string
		isRoomClosed     bool
		userID           UserID
		expectedResponse RoomCommandResponse
		ValidUserIDs     bool
	}{
		{
			name:             "room is closed",
			isRoomClosed:     true,
			userID:           1,
			expectedResponse: RoomCommandResponse{Err: NewRoomError(RoomErrorRoomClosed), content: RoomCommandContentNone},
		},
		{
			name:             "user is allowed to join",
			isRoomClosed:     false,
			userID:           1,
			expectedResponse: RoomCommandResponse{content: RoomCommandContentAllowedToJoin, Err: nil},
			ValidUserIDs:     true,
		},
		{
			name:             "user is not allowed to join",
			isRoomClosed:     false,
			userID:           2,
			expectedResponse: RoomCommandResponse{content: RoomCommandContentNoPermissionToJoin, Err: NewRoomError(RoomErrorPermissionDenied)},
			ValidUserIDs:     false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			room := buildRoom(t, tc.isRoomClosed)
			if tc.ValidUserIDs {
				responseChan := make(chan RoomCommandResponse, 1)
				addPlayerCommand := AddPlayerCommand{ID: tc.userID, OutputChan: responseChan}
				room.ExternalIncomingMessages <- addPlayerCommand
				assertRoomCommandResponse(RoomCommandResponse{content: RoomCommandContentPermissionToJoinGame, Err: nil}, responseChan, t)
			}
			responseChan := make(chan RoomCommandResponse, 1)
			checkCommand := CheckIfUserAllowedToJoin{ID: tc.userID, OutputChan: responseChan}
			room.ExternalIncomingMessages <- checkCommand
			assertRoomCommandResponse(tc.expectedResponse, responseChan, t)
		})
	}
}

func TestRoom_AddPlayerToWebsocketCommand(t *testing.T) {
	t.Parallel()
	room := buildRoom(t, false)
	responseChan := make(chan RoomCommandResponse, 1)
	newMessageChan := make(chan Message, 100)
	addWebsocketCommand := AddPlayerToWebsocketCommand{ID: 1, OutputChan: responseChan, NewMessageChan: newMessageChan}
	room.ExternalIncomingMessages <- addWebsocketCommand
	assertRoomCommandResponse(RoomCommandResponse{content: RoomCommandContentAddedWebsocket, Err: nil}, responseChan, t)
	// make sure a new message is able to be read
	room.InternalIncomingMessages <- "test message"
	assertRoomNewMessage("test message", newMessageChan, t)
}

func TestRoom_UserDisconnection(t *testing.T) {
	t.Parallel()
	room := buildRoom(t, false)
	responseChan := make(chan RoomCommandResponse, 1)
	newMessageChan := make(chan Message, 100)
	addWebsocketCommand := AddPlayerToWebsocketCommand{ID: 1, OutputChan: responseChan, NewMessageChan: newMessageChan}
	room.ExternalIncomingMessages <- addWebsocketCommand
	assertRoomCommandResponse(RoomCommandResponse{content: RoomCommandContentAddedWebsocket, Err: nil}, responseChan, t)
	// now disconnect the user and make sure no new message is able to be read
	room.InternalIncomingMessages <- UserDisconnection{ID: 1}
	room.InternalIncomingMessages <- "test message"
	assertNoNewRoomMessage(newMessageChan, t)
}

func TestRoom_MultipleListeners(t *testing.T) {
	t.Parallel()
	room := buildRoom(t, false)
	usersChan := make(map[UserID]chan Message)
	for range 10 {
		userID := UserID(len(usersChan) + 1)
		responseChan := make(chan RoomCommandResponse, 1)
		newMessageChan := make(chan Message, 100)
		addWebsocketCommand := AddPlayerToWebsocketCommand{ID: userID, OutputChan: responseChan, NewMessageChan: newMessageChan}
		room.ExternalIncomingMessages <- addWebsocketCommand
		assertRoomCommandResponse(RoomCommandResponse{content: RoomCommandContentAddedWebsocket, Err: nil}, responseChan, t)
		usersChan[userID] = newMessageChan
	}
	room.InternalIncomingMessages <- "test message"
	for _, userChan := range usersChan {
		assertRoomNewMessage("test message", userChan, t)
	}
}

func TestQuestion_MultipleQuestionsAsked(t *testing.T) {
	t.Parallel()
	manager, _ := buildManager(t, 3, defaultWaitTime)
	// NOTE: we are only testing the question asking part of the manager here, so we will just read from the ConnectionToWebsocketChan and make sure we get the expected questions in order
	for index := range 3 {
		assrtQuestionToWebsocket(manager.Questions[index], manager.ConnectionToWebsocketChan, t)
	}
}

func TestQuestion_CorrectAggreationOfResults(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name           string
		users          []UserID
		usersAnswers   map[UserID]string
		expectedResult map[bool][]UserID
	}{
		{
			name:  "all users answer correctly",
			users: []UserID{1, 2, 3},
			usersAnswers: map[UserID]string{
				1: "4",
				2: "4",
				3: "4",
			},
			expectedResult: map[bool][]UserID{
				true:  {1, 2, 3},
				false: {},
			},
		},
		{
			name:  "some users answer correctly",
			users: []UserID{1, 2, 3},
			usersAnswers: map[UserID]string{
				1: "4",
				2: "5",
				3: "4",
			},
			expectedResult: map[bool][]UserID{
				true:  {1, 3},
				false: {2},
			},
		},
		{
			name:  "no users answer correctly",
			users: []UserID{1, 2, 3},
			usersAnswers: map[UserID]string{
				1: "5",
				2: "5",
				3: "5",
			},
			expectedResult: map[bool][]UserID{
				true:  {},
				false: {1, 2, 3},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			manager, userResponseChan := buildManager(t, 1, defaultWaitTime)
			for userID, answer := range tc.usersAnswers {
				responseChan := make(chan QuestionAcknowledgmentResponse, 1)
				userResponse := UserResponse{
					UserID:       int32(userID),
					QuestionID:   manager.Questions[0].QuestionID,
					Answer:       answer,
					ResponseChan: responseChan,
				}
				userResponseChan <- userResponse
				assertQuestionAcknowledgmentResponse(QuestionAcknowledgmentResponse{Registered: true}, responseChan, t)
			}
			// now verify that the manager sends back the correct aggreation of results
			assertQuestionAggreatatedResults(QuestionAnsweredResults{
				CorrectUsers:   tc.expectedResult[true],
				IncorrectUsers: tc.expectedResult[false],
			}, manager.ConnectionToWebsocketChan, t)
		})
	}
}

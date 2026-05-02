package ventionroom

import (
	"context"
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

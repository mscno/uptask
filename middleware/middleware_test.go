package middleware

//
//import (
//	"context"
//	cloudevents "github.com/cloudevents/sdk-go/v2"
//	"github.com/samber/oops"
//	"github.com/stretchr/testify/require"
//	"github.com/znowdev/znow-go/pkg/constants"
//	"github.com/znowdev/znow-go/pkg/tasks/events"
//	"github.com/znowdev/znow-go/pkg/testutl"
//	"github.com/znowdev/znow-go/pkg/userctx"
//	"github.com/znowdev/znow-go/pkg/uuid"
//	"testing"
//)
//
//type dummyProcessor struct {
//	event *cloudevents.Event
//}
//
//func (d *dummyProcessor) ProcessTask(ctx context.Context, ce *cloudevents.Event) error {
//	_, ok := userctx.FromContext(ctx)
//	if !ok {
//		return oops.In("dummyProcessor").Errorf("user context not found")
//	}
//
//	if _, ok := ctx.Value(constants.OperationContextKey).(string); !ok {
//		return oops.In("dummyProcessor").Errorf("operation context not found")
//	}
//	d.event = ce
//	return nil
//}
//
//func TestCloudEventHelpers(t *testing.T) {
//	id := uuid.NewString()
//
//	ctx := context.Background()
//	ctx = userctx.NewUserContext(ctx, &userctx.UserContext{
//		OrganizationId: uuid.NewString(),
//		DataLocation:   testutl.RandomRegion().ZnowRegionName,
//	})
//
//	payload, err := events.SerializeWithExt(ctx, id, "dummy", &testutl.DummyMsgpStruct{Name: "test"}, constants.OperationContextKey, uuid.NewString())
//	require.NoError(t, err)
//
//	ce := cloudevents.NewEvent(cloudevents.VersionV1)
//	err = ce.UnmarshalJSON(payload)
//	require.NoError(t, err)
//	proc := &dummyProcessor{}
//	handler := events.Handler(events.HandleFunc(proc.ProcessTask))
//	handler = SetOperationContext(handler)
//	handler = SetTaskContext(handler)
//	err = handler.ProcessEvent(context.Background(), &ce)
//	require.NoError(t, err)
//}

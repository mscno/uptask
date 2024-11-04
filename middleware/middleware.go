package middleware

import (
	"context"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/mscno/uptask/events"
)

//func SetOperationContext(h events.Handler) events.Handler {
//	return events.HandleFunc(func(ctx context.Context, ce *cloudevents.Event) error {
//		if op, ok := ce.Extensions()[constants.OperationContextKey].(string); ok {
//			ctx = context.WithValue(ctx, constants.OperationContextKey, op)
//		}
//		return h.ProcessEvent(ctx, ce)
//	})
//}

func SetTaskContext(h events.Handler) events.Handler {
	return events.HandleFunc(func(ctx context.Context, ce *cloudevents.Event) error {

		return h.ProcessEvent(ctx, ce)
	})
}

//
//func NewOperationResultHandler(phxMessenger *phx.Messenger, opsSvc *operations.Service) MiddlewareFunc {
//	return func(h events.Handler) events.Handler {
//		return events.HandleFunc(func(ctx context.Context, ce *cloudevents.Event) error {
//			buf := bytes.NewBuffer(nil)
//			defer buf.Reset()
//			ctx = context.WithValue(ctx, constants.ResponseWriterContextKey, buf)
//			err := h.ProcessEvent(ctx, ce)
//			if op, ok := ce.Extensions()[constants.OperationContextKey].(string); ok {
//				operation := &longrunningpb.Operation{Name: op, Done: true}
//				if err != nil {
//					operation.Result = &longrunningpb.Operation_Error{Error: &status.Status{
//						Code:    int32(codes.Internal),
//						Message: err.Error(),
//						Details: nil,
//					}}
//				} else {
//					// TODO Handle results
//					var res = new(anypb.Any)
//					err = proto.Unmarshal(buf.Bytes(), res)
//					if err != nil {
//						return oops.Wrapf(err, "failed to unmarshal result")
//					}
//					operation.Result = &longrunningpb.Operation_Response{Response: res}
//				}
//
//				_, err := opsSvc.UpdateOperation(ctx, &longrunningpb.UpdateOperationRequest{Operation: operation, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"done", "error", "response"}}})
//				if err != nil {
//					return err
//				}
//
//				err = phxMessenger.SendEvent(ctx, formatPhxEvent(ctx, ce, operation))
//				if err != nil {
//					return err
//				}
//			}
//			return err
//		})
//	}
//}

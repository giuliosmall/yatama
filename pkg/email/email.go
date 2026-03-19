package email

import (
	"context"
	"log/slog"
	"time"
)

// SendEmail simulates sending an email. It logs the received parameters,
// waits for 3 seconds (or until the context is cancelled), and returns.
// The function signature matches task.TaskFunc.
func SendEmail(ctx context.Context, params map[string]string) error {
	slog.InfoContext(ctx, "sending email", "to", params["to"], "subject", params["subject"])

	select {
	case <-ctx.Done():
		slog.WarnContext(ctx, "send email cancelled")
		return ctx.Err()
	case <-time.After(3 * time.Second):
	}

	slog.InfoContext(ctx, "email sent successfully", "to", params["to"])
	return nil
}

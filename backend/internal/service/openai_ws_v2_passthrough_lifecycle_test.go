package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

type stagedPassthroughFrame struct {
	messageType coderws.MessageType
	payload     []byte
}

type stagedPassthroughConn struct {
	frames    chan stagedPassthroughFrame
	writes    chan []byte
	closed    chan struct{}
	closeOnce sync.Once
}

func newStagedPassthroughConn() *stagedPassthroughConn {
	return &stagedPassthroughConn{
		frames: make(chan stagedPassthroughFrame, 4),
		writes: make(chan []byte, 4),
		closed: make(chan struct{}),
	}
}

func (c *stagedPassthroughConn) Send(payload string) {
	c.frames <- stagedPassthroughFrame{messageType: coderws.MessageText, payload: []byte(payload)}
}

func (c *stagedPassthroughConn) WriteJSON(context.Context, any) error { return nil }

func (c *stagedPassthroughConn) ReadMessage(ctx context.Context) ([]byte, error) {
	_, payload, err := c.ReadFrame(ctx)
	return payload, err
}

func (c *stagedPassthroughConn) Ping(context.Context) error { return nil }

func (c *stagedPassthroughConn) ReadFrame(ctx context.Context) (coderws.MessageType, []byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return coderws.MessageText, nil, ctx.Err()
	case <-c.closed:
		return coderws.MessageText, nil, errOpenAIWSConnClosed
	case frame := <-c.frames:
		return frame.messageType, append([]byte(nil), frame.payload...), nil
	}
}

func (c *stagedPassthroughConn) WriteFrame(ctx context.Context, _ coderws.MessageType, payload []byte) error {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.closed:
		return errOpenAIWSConnClosed
	default:
	}
	var parsed any
	if err := json.Unmarshal(payload, &parsed); err != nil {
		return err
	}
	select {
	case c.writes <- append([]byte(nil), payload...):
	case <-ctx.Done():
		return ctx.Err()
	case <-c.closed:
		return errOpenAIWSConnClosed
	}
	return nil
}

func (c *stagedPassthroughConn) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return nil
}

type stagedPassthroughDialer struct {
	conn openAIWSClientConn
}

func (d *stagedPassthroughDialer) Dial(context.Context, string, http.Header, string) (openAIWSClientConn, int, http.Header, error) {
	return d.conn, http.StatusSwitchingProtocols, http.Header{}, nil
}

func newPassthroughLifecycleService(cfg *config.Config, upstream *stagedPassthroughConn) *OpenAIGatewayService {
	return &OpenAIGatewayService{
		cfg:                       cfg,
		httpUpstream:              &httpUpstreamRecorder{},
		cache:                     &stubGatewayCache{},
		openaiWSResolver:          NewOpenAIWSProtocolResolver(cfg),
		toolCorrector:             NewCodexToolCorrector(),
		openaiWSPassthroughDialer: &stagedPassthroughDialer{conn: upstream},
	}
}

func passthroughLifecycleConfig() *config.Config {
	cfg := &config.Config{}
	cfg.Security.URLAllowlist.Enabled = false
	cfg.Security.URLAllowlist.AllowInsecureHTTP = true
	cfg.Gateway.OpenAIFirstOutputTimeoutSeconds = 1
	cfg.Gateway.OpenAIWS.Enabled = true
	cfg.Gateway.OpenAIWS.APIKeyEnabled = true
	cfg.Gateway.OpenAIWS.ResponsesWebsocketsV2 = true
	cfg.Gateway.OpenAIWS.ModeRouterV2Enabled = true
	cfg.Gateway.OpenAIWS.IngressModeDefault = OpenAIWSIngressModeCtxPool
	cfg.Gateway.OpenAIWS.IngressInterTurnIdleTimeoutSeconds = 1
	cfg.Gateway.OpenAIWS.DialTimeoutSeconds = 3
	cfg.Gateway.OpenAIWS.ReadTimeoutSeconds = 1
	cfg.Gateway.OpenAIWS.WriteTimeoutSeconds = 3
	return cfg
}

func passthroughLifecycleAccount() *Account {
	return &Account{
		ID:          901,
		Name:        "passthrough-lifecycle",
		Platform:    PlatformOpenAI,
		Type:        AccountTypeAPIKey,
		Status:      StatusActive,
		Schedulable: true,
		Concurrency: 1,
		Credentials: map[string]any{"api_key": "sk-test"},
		Extra: map[string]any{
			"openai_apikey_responses_websockets_v2_mode": OpenAIWSIngressModePassthrough,
		},
	}
}

func startPassthroughLifecycleServer(
	t *testing.T,
	controlCtx context.Context,
	svc *OpenAIGatewayService,
	account *Account,
	hooks ...*OpenAIWSIngressHooks,
) (*httptest.Server, <-chan error) {
	t.Helper()
	serverErr := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := coderws.Accept(w, r, &coderws.AcceptOptions{CompressionMode: coderws.CompressionContextTakeover})
		if err != nil {
			serverErr <- err
			return
		}
		defer func() { _ = conn.CloseNow() }()

		msgType, firstMessage, err := ReadOpenAIWSClientMessage(
			controlCtx,
			conn,
			3*time.Second,
			coderws.StatusPolicyViolation,
			"missing first response.create message",
		)
		if err != nil {
			serverErr <- err
			return
		}
		if msgType != coderws.MessageText {
			serverErr <- errors.New("first message was not text")
			return
		}

		recorder := httptest.NewRecorder()
		ginCtx, _ := gin.CreateTestContext(recorder)
		req := r.Clone(controlCtx)
		req.Header = req.Header.Clone()
		ginCtx.Request = req
		var ingressHooks *OpenAIWSIngressHooks
		if len(hooks) > 0 {
			ingressHooks = hooks[0]
		}
		serverErr <- svc.ProxyResponsesWebSocketFromClient(controlCtx, ginCtx, conn, account, "sk-test", firstMessage, ingressHooks)
	}))
	return server, serverErr
}

func dialPassthroughLifecycleClient(t *testing.T, server *httptest.Server) *coderws.Conn {
	t.Helper()
	dialCtx, cancelDial := context.WithTimeout(context.Background(), 3*time.Second)
	clientConn, _, err := coderws.Dial(dialCtx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	cancelDial()
	require.NoError(t, err)
	writeCtx, cancelWrite := context.WithTimeout(context.Background(), 3*time.Second)
	err = clientConn.Write(writeCtx, coderws.MessageText, []byte(`{"type":"response.create","model":"gpt-5.1","stream":false}`))
	cancelWrite()
	require.NoError(t, err)
	return clientConn
}

func readPassthroughLifecycleFrame(t *testing.T, clientConn *coderws.Conn, timeout time.Duration) ([]byte, error) {
	t.Helper()
	readCtx, cancelRead := context.WithTimeout(context.Background(), timeout)
	_, payload, err := clientConn.Read(readCtx)
	cancelRead()
	return payload, err
}

func requirePassthroughUpstreamWrite(t *testing.T, upstream *stagedPassthroughConn, timeout time.Duration) []byte {
	t.Helper()
	select {
	case payload := <-upstream.writes:
		return payload
	case <-time.After(timeout):
		t.Fatal("passthrough request was not forwarded upstream")
		return nil
	}
}

func TestOpenAIWSPassthroughSilentDisconnectReleasesAfterUpstreamClose(t *testing.T) {
	cfg := passthroughLifecycleConfig()
	cfg.Gateway.OpenAIFirstOutputTimeoutSeconds = 60
	cfg.Gateway.OpenAIWS.ReadTimeoutSeconds = 60
	upstream := newStagedPassthroughConn()
	after := make(chan bool, 2)
	cleanupErrors := make(chan error, 2)
	cleanupResults := make(chan *OpenAIForwardResult, 2)
	hooks := &OpenAIWSIngressHooks{AfterTurn: func(_ int, result *OpenAIForwardResult, err error) {
		cleanupErrors <- err
		cleanupResults <- result
		select {
		case <-upstream.closed:
			after <- true
		default:
			after <- false
		}
	}}
	server, done := startPassthroughLifecycleServer(t, context.Background(), newPassthroughLifecycleService(cfg, upstream), passthroughLifecycleAccount(), hooks)
	defer server.Close()
	client := dialPassthroughLifecycleClient(t, server)
	requirePassthroughUpstreamWrite(t, upstream, time.Second)
	require.NoError(t, client.CloseNow())
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(4 * time.Second):
		t.Fatal("silent upstream retained turn")
	}
	require.True(t, <-after, "upstream must close before slot cleanup")
	require.NoError(t, <-cleanupErrors)
	require.Nil(t, <-cleanupResults)
	require.Empty(t, after, "cleanup must run once")
}

type ingressDrainTestConn struct{ *stagedPassthroughConn }

func (c *ingressDrainTestConn) WriteJSON(ctx context.Context, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return c.WriteFrame(ctx, coderws.MessageText, payload)
}

func TestOpenAIGatewayService_ProxyResponsesWebSocketFromClient_SilentDisconnectDrain(t *testing.T) {
	for _, terminal := range []bool{false, true} {
		t.Run(fmt.Sprint("terminal=", terminal), func(t *testing.T) {
			cfg := passthroughLifecycleConfig()
			cfg.Gateway.OpenAIWS.ReadTimeoutSeconds = 60
			upstream := newStagedPassthroughConn()
			pool := newOpenAIWSConnPool(cfg)
			pool.setClientDialerForTest(&stagedPassthroughDialer{conn: &ingressDrainTestConn{upstream}})
			defer pool.Close()
			svc := newPassthroughLifecycleService(cfg, upstream)
			svc.openaiWSPool = pool
			account := passthroughLifecycleAccount()
			account.Extra["openai_apikey_responses_websockets_v2_mode"] = OpenAIWSIngressModeCtxPool
			after := make(chan *OpenAIForwardResult, 2)
			cleanupErrors := make(chan error, 2)
			closedAtCleanup := make(chan bool, 2)
			hooks := &OpenAIWSIngressHooks{AfterTurn: func(_ int, result *OpenAIForwardResult, err error) {
				cleanupErrors <- err
				select {
				case <-upstream.closed:
					closedAtCleanup <- true
				default:
					closedAtCleanup <- false
				}
				after <- result
			}}
			server, done := startPassthroughLifecycleServer(t, context.Background(), svc, account, hooks)
			defer server.Close()
			client := dialPassthroughLifecycleClient(t, server)
			requirePassthroughUpstreamWrite(t, upstream, time.Second)
			require.NoError(t, client.CloseNow())
			if terminal {
				upstream.Send(`{"type":"response.completed","response":{"id":"resp_drain","usage":{"input_tokens":9,"output_tokens":2}}}`)
			}
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(4 * time.Second):
				t.Fatal("silent ctx_pool upstream retained turn")
			}
			closed := <-closedAtCleanup
			if !terminal {
				require.True(t, closed, "unfinished upstream must close before cleanup")
			}
			result := <-after
			require.NoError(t, <-cleanupErrors)
			if terminal {
				require.NotNil(t, result)
				require.Equal(t, 9, result.Usage.InputTokens)
			} else {
				require.Nil(t, result, "no usage result may be fabricated")
			}
			require.Empty(t, after)
		})
	}
}

func TestOpenAIWSPassthroughLaterTurnAdmission(t *testing.T) {
	for _, reject := range []bool{false, true} {
		t.Run(fmt.Sprint("reject=", reject), func(t *testing.T) {
			cfg := passthroughLifecycleConfig()
			upstream := newStagedPassthroughConn()
			before := make(chan int, 2)
			after := make(chan int, 3)
			hooks := &OpenAIWSIngressHooks{
				BeforeTurn: func(turn int) error {
					before <- turn
					if reject {
						return errors.New("api key concurrency exhausted")
					}
					return nil
				},
				AfterTurn: func(turn int, _ *OpenAIForwardResult, _ error) { after <- turn },
			}
			server, done := startPassthroughLifecycleServer(t, context.Background(), newPassthroughLifecycleService(cfg, upstream), passthroughLifecycleAccount(), hooks)
			defer server.Close()
			client := dialPassthroughLifecycleClient(t, server)
			defer client.CloseNow()
			requirePassthroughUpstreamWrite(t, upstream, time.Second)
			upstream.Send(`{"type":"response.completed","response":{"id":"resp_1","usage":{"input_tokens":4,"output_tokens":2}}}`)
			_, err := readPassthroughLifecycleFrame(t, client, time.Second)
			require.NoError(t, err)
			require.Equal(t, 1, <-after)
			require.NoError(t, client.Write(context.Background(), coderws.MessageText, []byte(`{"type":"response.create","model":"gpt-5.1"}`)))
			select {
			case turn := <-before:
				require.Equal(t, 2, turn)
			case <-time.After(time.Second):
				t.Fatal("missing later turn admission")
			}
			if reject {
				_ = client.CloseNow()
				select {
				case err := <-done:
					require.ErrorContains(t, err, "api key concurrency exhausted")
				case <-time.After(4 * time.Second):
					t.Fatal("rejected relay did not stop")
				}
				require.Empty(t, upstream.writes)
				require.Empty(t, after, "unadmitted turn must not release another turn's slots")
			} else {
				requirePassthroughUpstreamWrite(t, upstream, time.Second)
				upstream.Send(`{"type":"response.completed","response":{"id":"resp_2","usage":{"input_tokens":7,"output_tokens":3}}}`)
				_, err = readPassthroughLifecycleFrame(t, client, time.Second)
				require.NoError(t, err)
				require.Equal(t, 2, <-after)
				_ = client.CloseNow()
				select {
				case <-done:
				case <-time.After(4 * time.Second):
					t.Fatal("relay did not stop")
				}
			}
		})
	}
}

func TestOpenAIWSPassthroughActiveTurnFailureCleanup(t *testing.T) {
	for _, failure := range []string{"overlap", "cancellation"} {
		t.Run(failure, func(t *testing.T) {
			cfg := passthroughLifecycleConfig()
			cfg.Gateway.OpenAIFirstOutputTimeoutSeconds = 60
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			upstream := newStagedPassthroughConn()
			after := make(chan int, 2)
			before := make(chan int, 2)
			closed := make(chan bool, 2)
			hooks := &OpenAIWSIngressHooks{
				BeforeTurn: func(turn int) error { before <- turn; return nil },
				AfterTurn: func(turn int, _ *OpenAIForwardResult, _ error) {
					select {
					case <-upstream.closed:
						closed <- true
					default:
						closed <- false
					}
					after <- turn
				},
			}
			server, done := startPassthroughLifecycleServer(t, ctx, newPassthroughLifecycleService(cfg, upstream), passthroughLifecycleAccount(), hooks)
			defer server.Close()
			client := dialPassthroughLifecycleClient(t, server)
			defer client.CloseNow()
			requirePassthroughUpstreamWrite(t, upstream, time.Second)
			if failure == "overlap" {
				require.NoError(t, client.Write(context.Background(), coderws.MessageText, []byte(`{"type":"response.create","model":"gpt-5.1"}`)))
			} else {
				cancel()
			}
			_, err := readPassthroughLifecycleFrame(t, client, 4*time.Second)
			require.Error(t, err)
			select {
			case err := <-done:
				require.Error(t, err)
			case <-time.After(4 * time.Second):
				t.Fatal("failed relay retained turn")
			}
			require.Equal(t, 1, <-after)
			require.True(t, <-closed)
			require.Empty(t, before)
			require.Empty(t, after)
			require.Empty(t, upstream.writes)
		})
	}
}

func TestOpenAIWSPassthroughTurnLifecycle_SerializesTerminalCommitAndNextTurn(t *testing.T) {
	clientFrameConn := &openAIWSClientFrameConn{interTurnStarted: make(chan struct{}, 1)}
	clientFrameConn.markTurnCompleted()
	lifecycle := newOpenAIWSPassthroughTurnLifecycle(true)
	lifecycle.beginTerminalWrite()

	admitted := make(chan bool, 1)
	go func() {
		admitted <- lifecycle.beginResponseCreate(clientFrameConn.markTurnStarted)
	}()
	select {
	case <-admitted:
		t.Fatal("next response.create was admitted before terminal commit completed")
	case <-time.After(50 * time.Millisecond):
	}

	lifecycle.finishTerminalWrite(true, clientFrameConn.markTurnCompleted)
	select {
	case ok := <-admitted:
		require.True(t, ok)
	case <-time.After(time.Second):
		t.Fatal("next response.create remained blocked after terminal commit")
	}
	require.False(t, clientFrameConn.waitingForNextTurn.Load(), "accepted next turn must win over terminal idle state")

	lifecycle = newOpenAIWSPassthroughTurnLifecycle(true)
	lifecycle.beginTerminalWrite()
	admitted = make(chan bool, 1)
	go func() {
		admitted <- lifecycle.beginResponseCreate(nil)
	}()
	lifecycle.finishTerminalWrite(false, func() {
		t.Error("failed terminal write must not commit idle state")
	})
	require.False(t, <-admitted, "failed terminal write must keep the current turn in flight")
}

func TestPassthroughLifecycle_LeaseLossSendsRetryClose(t *testing.T) {
	gin.SetMode(gin.TestMode)
	controlCtx, cancelControl := context.WithCancelCause(context.Background())
	upstream := newStagedPassthroughConn()
	upstream.Send(`{"type":"response.created","response":{"id":"resp_lease","model":"gpt-5.1"}}`)
	server, serverErr := startPassthroughLifecycleServer(t, controlCtx, newPassthroughLifecycleService(passthroughLifecycleConfig(), upstream), passthroughLifecycleAccount())
	defer server.Close()
	clientConn := dialPassthroughLifecycleClient(t, server)
	defer func() { _ = clientConn.CloseNow() }()

	event, err := readPassthroughLifecycleFrame(t, clientConn, 3*time.Second)
	require.NoError(t, err)
	require.Equal(t, "response.created", gjson.GetBytes(event, "type").String())
	cancelControl(ErrOpenAIWSIngressLeaseLost)

	_, err = readPassthroughLifecycleFrame(t, clientConn, 3*time.Second)
	var closeErr coderws.CloseError
	require.ErrorAs(t, err, &closeErr)
	require.Equal(t, coderws.StatusTryAgainLater, closeErr.Code)
	require.Equal(t, "websocket ingress capacity lease lost; please reconnect", closeErr.Reason)
	select {
	case <-serverErr:
	case <-time.After(3 * time.Second):
		t.Fatal("passthrough lease-loss reader did not exit")
	}
}

func TestPassthroughLifecycle_CompletedTurnStartsInterTurnIdle(t *testing.T) {
	gin.SetMode(gin.TestMode)
	controlCtx, cancelControl := context.WithCancelCause(context.Background())
	defer cancelControl(context.Canceled)
	upstream := newStagedPassthroughConn()
	upstream.Send(`{"type":"response.completed","response":{"id":"resp_idle","model":"gpt-5.1","usage":{"input_tokens":1,"output_tokens":1}}}`)
	server, serverErr := startPassthroughLifecycleServer(t, controlCtx, newPassthroughLifecycleService(passthroughLifecycleConfig(), upstream), passthroughLifecycleAccount())
	defer server.Close()
	clientConn := dialPassthroughLifecycleClient(t, server)
	defer func() { _ = clientConn.CloseNow() }()

	event, err := readPassthroughLifecycleFrame(t, clientConn, 3*time.Second)
	require.NoError(t, err)
	require.Equal(t, "response.completed", gjson.GetBytes(event, "type").String())
	_, err = readPassthroughLifecycleFrame(t, clientConn, 3*time.Second)
	var closeErr coderws.CloseError
	require.ErrorAs(t, err, &closeErr)
	require.Equal(t, coderws.StatusNormalClosure, closeErr.Code)
	require.Equal(t, "websocket idle timeout", closeErr.Reason)
	select {
	case <-serverErr:
	case <-time.After(3 * time.Second):
		t.Fatal("passthrough idle reader did not exit")
	}
}

func TestPassthroughLifecycle_ActiveTurnInactivityUsesReadTimeout(t *testing.T) {
	gin.SetMode(gin.TestMode)
	controlCtx, cancelControl := context.WithCancelCause(context.Background())
	defer cancelControl(context.Canceled)
	upstream := newStagedPassthroughConn()
	upstream.Send(`{"type":"response.output_text.delta","response_id":"resp_active","delta":"hello"}`)
	server, serverErr := startPassthroughLifecycleServer(t, controlCtx, newPassthroughLifecycleService(passthroughLifecycleConfig(), upstream), passthroughLifecycleAccount())
	defer server.Close()
	clientConn := dialPassthroughLifecycleClient(t, server)
	defer func() { _ = clientConn.CloseNow() }()

	delta, err := readPassthroughLifecycleFrame(t, clientConn, 3*time.Second)
	require.NoError(t, err)
	require.Equal(t, "response.output_text.delta", gjson.GetBytes(delta, "type").String())
	_, err = readPassthroughLifecycleFrame(t, clientConn, 2500*time.Millisecond)
	var websocketCloseErr coderws.CloseError
	require.ErrorAs(t, err, &websocketCloseErr)
	require.Equal(t, coderws.StatusGoingAway, websocketCloseErr.Code)
	require.Equal(t, "upstream websocket read timeout; please reconnect", websocketCloseErr.Reason)
	select {
	case err := <-serverErr:
		var closeErr *OpenAIWSClientCloseError
		require.ErrorAs(t, err, &closeErr)
		require.Equal(t, coderws.StatusGoingAway, closeErr.StatusCode())
		require.Equal(t, "upstream websocket read timeout; please reconnect", closeErr.Reason())
	case <-time.After(2500 * time.Millisecond):
		t.Fatal("passthrough active turn remained unbounded after upstream activity stopped")
	}
}

func TestPassthroughLifecycle_PreambleAllowsPromptClientCancel(t *testing.T) {
	gin.SetMode(gin.TestMode)
	controlCtx, cancelControl := context.WithCancelCause(context.Background())
	defer cancelControl(context.Canceled)
	cfg := passthroughLifecycleConfig()
	cfg.Gateway.OpenAIFirstOutputTimeoutSeconds = 3
	upstream := newStagedPassthroughConn()
	upstream.Send(`{"type":"response.created","response":{"id":"resp_cancel","model":"gpt-5.1"}}`)
	server, serverErr := startPassthroughLifecycleServer(t, controlCtx, newPassthroughLifecycleService(cfg, upstream), passthroughLifecycleAccount())
	defer server.Close()
	clientConn := dialPassthroughLifecycleClient(t, server)
	defer func() { _ = clientConn.CloseNow() }()
	require.Equal(t, "response.create", gjson.GetBytes(requirePassthroughUpstreamWrite(t, upstream, time.Second), "type").String())

	created, err := readPassthroughLifecycleFrame(t, clientConn, time.Second)
	require.NoError(t, err)
	require.Equal(t, "response.created", gjson.GetBytes(created, "type").String())
	writeCtx, cancelWrite := context.WithTimeout(context.Background(), time.Second)
	err = clientConn.Write(writeCtx, coderws.MessageText, []byte(`{"type":"response.cancel","response_id":"resp_cancel"}`))
	cancelWrite()
	require.NoError(t, err)
	cancelFrame := requirePassthroughUpstreamWrite(t, upstream, 500*time.Millisecond)
	require.Equal(t, "response.cancel", gjson.GetBytes(cancelFrame, "type").String())

	require.NoError(t, clientConn.Close(coderws.StatusNormalClosure, "done"))
	select {
	case <-serverErr:
	case <-time.After(3 * time.Second):
		t.Fatal("passthrough cancel test did not exit")
	}
}

func TestPassthroughLifecycle_RejectsOverlappingResponseCreate(t *testing.T) {
	gin.SetMode(gin.TestMode)
	controlCtx, cancelControl := context.WithCancelCause(context.Background())
	defer cancelControl(context.Canceled)
	cfg := passthroughLifecycleConfig()
	cfg.Gateway.OpenAIFirstOutputTimeoutSeconds = 3
	upstream := newStagedPassthroughConn()
	upstream.Send(`{"type":"response.created","response":{"id":"resp_overlap_first","model":"gpt-5.1"}}`)
	server, serverErr := startPassthroughLifecycleServer(t, controlCtx, newPassthroughLifecycleService(cfg, upstream), passthroughLifecycleAccount())
	defer server.Close()
	clientConn := dialPassthroughLifecycleClient(t, server)
	defer func() { _ = clientConn.CloseNow() }()
	require.Equal(t, "response.create", gjson.GetBytes(requirePassthroughUpstreamWrite(t, upstream, time.Second), "type").String())

	created, err := readPassthroughLifecycleFrame(t, clientConn, time.Second)
	require.NoError(t, err)
	require.Equal(t, "response.created", gjson.GetBytes(created, "type").String())
	writeCtx, cancelWrite := context.WithTimeout(context.Background(), time.Second)
	err = clientConn.Write(writeCtx, coderws.MessageText, []byte(`{"type":"response.create","model":"gpt-5.1"}`))
	cancelWrite()
	require.NoError(t, err)

	_, err = readPassthroughLifecycleFrame(t, clientConn, time.Second)
	var websocketCloseErr coderws.CloseError
	require.ErrorAs(t, err, &websocketCloseErr)
	require.Equal(t, coderws.StatusPolicyViolation, websocketCloseErr.Code)
	require.Equal(t, "overlapping response.create is not supported", websocketCloseErr.Reason)
	select {
	case err := <-serverErr:
		var closeErr *OpenAIWSClientCloseError
		require.ErrorAs(t, err, &closeErr)
		require.Equal(t, coderws.StatusPolicyViolation, closeErr.StatusCode())
		require.Equal(t, "overlapping response.create is not supported", closeErr.Reason())
	case <-time.After(3 * time.Second):
		t.Fatal("overlapping response.create did not terminate passthrough")
	}
}

func TestPassthroughLifecycle_ActiveTurnActivityRefreshesReadTimeout(t *testing.T) {
	gin.SetMode(gin.TestMode)
	controlCtx, cancelControl := context.WithCancelCause(context.Background())
	defer cancelControl(context.Canceled)
	upstream := newStagedPassthroughConn()
	upstream.Send(`{"type":"response.output_text.delta","response_id":"resp_active_refresh","delta":"one"}`)
	go func() {
		for _, event := range []string{
			`{"type":"response.output_text.delta","response_id":"resp_active_refresh","delta":"two"}`,
			`{"type":"response.output_text.delta","response_id":"resp_active_refresh","delta":"three"}`,
			`{"type":"response.completed","response":{"id":"resp_active_refresh","model":"gpt-5.1","usage":{"input_tokens":1,"output_tokens":3}}}`,
		} {
			timer := time.NewTimer(600 * time.Millisecond)
			<-timer.C
			timer.Stop()
			upstream.Send(event)
		}
	}()
	server, serverErr := startPassthroughLifecycleServer(t, controlCtx, newPassthroughLifecycleService(passthroughLifecycleConfig(), upstream), passthroughLifecycleAccount())
	defer server.Close()
	clientConn := dialPassthroughLifecycleClient(t, server)
	defer func() { _ = clientConn.CloseNow() }()

	for _, wantType := range []string{
		"response.output_text.delta",
		"response.output_text.delta",
		"response.output_text.delta",
		"response.completed",
	} {
		frame, err := readPassthroughLifecycleFrame(t, clientConn, 3*time.Second)
		require.NoError(t, err)
		require.Equal(t, wantType, gjson.GetBytes(frame, "type").String())
	}
	require.NoError(t, clientConn.Close(coderws.StatusNormalClosure, "done"))
	select {
	case <-serverErr:
	case <-time.After(3 * time.Second):
		t.Fatal("passthrough active-turn refresh test did not exit")
	}
}

func TestPassthroughLifecycle_TerminalSwitchesToInterTurnIdleTimeout(t *testing.T) {
	gin.SetMode(gin.TestMode)
	controlCtx, cancelControl := context.WithCancelCause(context.Background())
	defer cancelControl(context.Canceled)
	cfg := passthroughLifecycleConfig()
	cfg.Gateway.OpenAIWS.ReadTimeoutSeconds = 1
	cfg.Gateway.OpenAIWS.IngressInterTurnIdleTimeoutSeconds = 2
	upstream := newStagedPassthroughConn()
	upstream.Send(`{"type":"response.completed","response":{"id":"resp_idle_first","model":"gpt-5.1","usage":{"input_tokens":1,"output_tokens":1}}}`)
	server, serverErr := startPassthroughLifecycleServer(t, controlCtx, newPassthroughLifecycleService(cfg, upstream), passthroughLifecycleAccount())
	defer server.Close()
	clientConn := dialPassthroughLifecycleClient(t, server)
	defer func() { _ = clientConn.CloseNow() }()
	require.Equal(t, "response.create", gjson.GetBytes(requirePassthroughUpstreamWrite(t, upstream, 3*time.Second), "type").String())

	completed, err := readPassthroughLifecycleFrame(t, clientConn, 3*time.Second)
	require.NoError(t, err)
	require.Equal(t, "resp_idle_first", gjson.GetBytes(completed, "response.id").String())
	time.Sleep(1300 * time.Millisecond)
	writeCtx, cancelWrite := context.WithTimeout(context.Background(), 3*time.Second)
	err = clientConn.Write(writeCtx, coderws.MessageText, []byte(`{"type":"response.create","model":"gpt-5.1","previous_response_id":"resp_idle_first"}`))
	cancelWrite()
	require.NoError(t, err)
	require.Equal(t, "response.create", gjson.GetBytes(requirePassthroughUpstreamWrite(t, upstream, 3*time.Second), "type").String())
	upstream.Send(`{"type":"response.completed","response":{"id":"resp_idle_second","model":"gpt-5.1","usage":{"input_tokens":1,"output_tokens":1}}}`)
	completed, err = readPassthroughLifecycleFrame(t, clientConn, 3*time.Second)
	require.NoError(t, err)
	require.Equal(t, "resp_idle_second", gjson.GetBytes(completed, "response.id").String())
	_, err = readPassthroughLifecycleFrame(t, clientConn, 3*time.Second)
	var websocketCloseErr coderws.CloseError
	require.ErrorAs(t, err, &websocketCloseErr)
	require.Equal(t, coderws.StatusNormalClosure, websocketCloseErr.Code)
	require.Equal(t, "websocket idle timeout", websocketCloseErr.Reason)

	select {
	case err := <-serverErr:
		var closeErr *OpenAIWSClientCloseError
		require.ErrorAs(t, err, &closeErr)
		require.Equal(t, coderws.StatusNormalClosure, closeErr.StatusCode())
		require.Equal(t, "websocket idle timeout", closeErr.Reason())
	case <-time.After(3 * time.Second):
		t.Fatal("passthrough terminal turn did not use inter-turn idle timeout")
	}
}

func TestPassthroughLifecycle_FirstOutputTimeoutRemainsBounded(t *testing.T) {
	gin.SetMode(gin.TestMode)
	controlCtx, cancelControl := context.WithCancelCause(context.Background())
	defer cancelControl(context.Canceled)
	upstream := newStagedPassthroughConn()
	server, serverErr := startPassthroughLifecycleServer(t, controlCtx, newPassthroughLifecycleService(passthroughLifecycleConfig(), upstream), passthroughLifecycleAccount())
	defer server.Close()
	clientConn := dialPassthroughLifecycleClient(t, server)
	defer func() { _ = clientConn.CloseNow() }()

	select {
	case err := <-serverErr:
		var failoverErr *UpstreamFailoverError
		require.ErrorAs(t, err, &failoverErr)
		require.Equal(t, http.StatusGatewayTimeout, failoverErr.StatusCode)
		require.Contains(t, string(failoverErr.ResponseBody), "first_output_timeout")
	case <-time.After(2500 * time.Millisecond):
		t.Fatal("passthrough first output was left unbounded")
	}
}

func TestPassthroughLifecycle_ResponseCreatedTimeoutClosesWithoutFailover(t *testing.T) {
	gin.SetMode(gin.TestMode)
	controlCtx, cancelControl := context.WithCancelCause(context.Background())
	defer cancelControl(context.Canceled)
	upstream := newStagedPassthroughConn()
	upstream.Send(`{"type":"response.created","response":{"id":"resp_preamble","model":"gpt-5.1"}}`)
	server, serverErr := startPassthroughLifecycleServer(t, controlCtx, newPassthroughLifecycleService(passthroughLifecycleConfig(), upstream), passthroughLifecycleAccount())
	defer server.Close()
	clientConn := dialPassthroughLifecycleClient(t, server)
	defer func() { _ = clientConn.CloseNow() }()

	created, err := readPassthroughLifecycleFrame(t, clientConn, 3*time.Second)
	require.NoError(t, err)
	require.Equal(t, "response.created", gjson.GetBytes(created, "type").String())
	_, err = readPassthroughLifecycleFrame(t, clientConn, 2500*time.Millisecond)
	var websocketCloseErr coderws.CloseError
	require.ErrorAs(t, err, &websocketCloseErr)
	require.Equal(t, coderws.StatusGoingAway, websocketCloseErr.Code)
	require.Equal(t, "upstream produced no semantic output; please reconnect", websocketCloseErr.Reason)
	select {
	case err := <-serverErr:
		var failoverErr *UpstreamFailoverError
		require.NotErrorAs(t, err, &failoverErr)
		var closeErr *OpenAIWSClientCloseError
		require.ErrorAs(t, err, &closeErr)
		require.Equal(t, coderws.StatusGoingAway, closeErr.StatusCode())
		require.Equal(t, "upstream produced no semantic output; please reconnect", closeErr.Reason())
	case <-time.After(2500 * time.Millisecond):
		t.Fatal("response.created timeout did not close the passthrough connection")
	}
}

func TestPassthroughLifecycle_SecondTurnTimeoutIsNotFailoverSafe(t *testing.T) {
	gin.SetMode(gin.TestMode)
	controlCtx, cancelControl := context.WithCancelCause(context.Background())
	defer cancelControl(context.Canceled)
	upstream := newStagedPassthroughConn()
	upstream.Send(`{"type":"response.completed","response":{"id":"resp_first","model":"gpt-5.1","usage":{"input_tokens":1,"output_tokens":1}}}`)
	server, serverErr := startPassthroughLifecycleServer(t, controlCtx, newPassthroughLifecycleService(passthroughLifecycleConfig(), upstream), passthroughLifecycleAccount())
	defer server.Close()
	clientConn := dialPassthroughLifecycleClient(t, server)
	defer func() { _ = clientConn.CloseNow() }()

	completed, err := readPassthroughLifecycleFrame(t, clientConn, 3*time.Second)
	require.NoError(t, err)
	require.Equal(t, "response.completed", gjson.GetBytes(completed, "type").String())
	writeCtx, cancelWrite := context.WithTimeout(context.Background(), 3*time.Second)
	err = clientConn.Write(writeCtx, coderws.MessageText, []byte(`{"type":"response.create","model":"gpt-5.1","previous_response_id":"resp_first"}`))
	cancelWrite()
	require.NoError(t, err)
	upstream.Send(`{"type":"response.created","response":{"id":"resp_second","model":"gpt-5.1"}}`)

	created, err := readPassthroughLifecycleFrame(t, clientConn, 3*time.Second)
	require.NoError(t, err)
	require.Equal(t, "response.created", gjson.GetBytes(created, "type").String())
	_, err = readPassthroughLifecycleFrame(t, clientConn, 2500*time.Millisecond)
	var websocketCloseErr coderws.CloseError
	require.ErrorAs(t, err, &websocketCloseErr)
	require.Equal(t, coderws.StatusGoingAway, websocketCloseErr.Code)
	require.Equal(t, "upstream produced no semantic output; please reconnect", websocketCloseErr.Reason)
	select {
	case err := <-serverErr:
		var failoverErr *UpstreamFailoverError
		require.NotErrorAs(t, err, &failoverErr, "handler must not replay the initial request on another account for a later-turn timeout")
		var closeErr *OpenAIWSClientCloseError
		require.ErrorAs(t, err, &closeErr)
		require.Equal(t, coderws.StatusGoingAway, closeErr.StatusCode())
	case <-time.After(2500 * time.Millisecond):
		t.Fatal("second turn first semantic output was left unbounded")
	}
}

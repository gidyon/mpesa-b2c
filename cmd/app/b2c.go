package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"encoding/json"

	auth "github.com/gidyon/gomicro/pkg/grpc/auth"
	b2c_app_v1 "github.com/gidyon/mpesab2c/internal/b2c/v1"
	b2c_v1 "github.com/gidyon/mpesab2c/pkg/api/b2c/v1"
	"github.com/gidyon/mpesab2c/pkg/utils/httputils"
	"github.com/go-redis/redis/v8"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/metadata"
	"gorm.io/gorm"

	"github.com/gidyon/mpesab2c/pkg/payload"
	"google.golang.org/protobuf/proto"
)

type Options struct {
	SQLDB    *gorm.DB
	RedisDB  *redis.Client
	Logger   grpclog.LoggerV2
	AuthAPI  *auth.API
	B2CV1API b2c_v1.B2CV1Server
}

func validateOptions(opt *Options) error {
	var err error
	switch {
	case opt == nil:
		err = errors.New("missing options")
	case opt.SQLDB == nil:
		err = errors.New("missing sqlDB")
	case opt.RedisDB == nil:
		err = errors.New("missing redisDB")
	case opt.Logger == nil:
		err = errors.New("missing logger")
	case opt.AuthAPI == nil:
		err = errors.New("missing auth API")
	case opt.B2CV1API == nil:
		err = errors.New("missing b2c v1 API")
	}
	return err
}

type b2cGateway struct {
	*Options
	ctxExt context.Context
}

// NewB2CGateway creates a b2c gateway for receiving b2cPayloads
func NewB2CGateway(ctx context.Context, opt *Options) (*b2cGateway, error) {
	err := validateOptions(opt)
	if err != nil {
		return nil, err
	}

	gw := &b2cGateway{
		Options: opt,
	}

	// Generate token
	token, err := gw.AuthAPI.GenToken(
		ctx, &auth.Payload{Group: auth.DefaultAdminGroup()}, time.Now().Add(10*365*24*time.Hour))
	if err != nil {
		return nil, fmt.Errorf("failed to generate auth token: %v", err)
	}

	md := metadata.Pairs(auth.Header(), fmt.Sprintf("%s %s", auth.Scheme(), token))

	ctxExt := metadata.NewIncomingContext(ctx, md)

	// Authenticate the token
	gw.ctxExt, err = gw.AuthAPI.Authenticator(ctxExt)
	if err != nil {
		return nil, err
	}

	return gw, nil
}

func (gw *b2cGateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	code, err := gw.fromSaf(w, r)
	if err != nil {
		gw.Logger.Errorln("Incoming B2C failed: %s", err)
		http.Error(w, "request handler failed", code)
		return
	}
}

func (gw *b2cGateway) fromSaf(w http.ResponseWriter, r *http.Request) (int, error) {

	httputils.DumpRequest(r, "Incoming Mpesa B2C Payload V1")

	if r.Method != http.MethodPost {
		return http.StatusBadRequest, fmt.Errorf("bad method; only POST allowed; received %v method", r.Method)
	}

	var (
		b2cPayload = &payload.Transaction{}
		tranferReq = &b2c_v1.TransferFundsRequest{}
		db         = &b2c_app_v1.Payment{}
		succeeded  = "YES"
		status     = b2c_v1.B2CStatus_B2C_SUCCESS.String()
		err        error
	)

	// Marshal incoming payload
	switch strings.ToLower(r.Header.Get("content-type")) {
	case "application/json", "application/json;charset=utf-8", "application/json;charset=utf8":
		err = json.NewDecoder(r.Body).Decode(b2cPayload)
		if err != nil {
			return http.StatusBadRequest, fmt.Errorf("decoding json failed: %w", err)
		}
	default:
		ctype := r.Header.Get("content-type")
		return http.StatusBadRequest, fmt.Errorf("unexpected content type: %s", ctype)
	}

	// Validate incoming payload
	switch {
	case b2cPayload == nil:
		err = fmt.Errorf("missing b2cPayload")
	case b2cPayload.Result.ConversationID == "":
		err = fmt.Errorf("missing conversation id")
	case b2cPayload.Result.OriginatorConversationID == "":
		err = fmt.Errorf("missing originator id")
	case b2cPayload.Result.ResultDesc == "":
		err = fmt.Errorf("missing description")
	}
	if err != nil {
		return http.StatusBadRequest, err
	}

	if b2cPayload.Result.ResultCode != 0 {
		succeeded = "NO"
		status = b2c_v1.B2CStatus_B2C_FAILED.String()
	}

	ctx := r.Context()

	// Get tranfer funds request
	res, err := gw.RedisDB.Get(ctx, b2c_app_v1.GetMpesaRequestKey(b2cPayload.ConversationID())).Result()
	switch {
	case err == nil:
		err = proto.Unmarshal([]byte(res), tranferReq)
		if err != nil {
			gw.Logger.Errorln("Failed to unmarshal transfer funds request: ", err)
		}
	}

	err = gw.SQLDB.First(db, "conversation_id = ?", b2cPayload.ConversationID()).Error
	switch {
	case err == nil:
		// Update STK b2cPayload
		err = gw.SQLDB.Model(db).
			Updates(map[string]interface{}{
				"result_code":           fmt.Sprint(b2cPayload.Result.ResultCode),
				"result_description":    b2cPayload.Result.ResultDesc,
				"working_account_funds": float32(b2cPayload.B2CWorkingAccountAvailableFunds()),
				"utility_account_funds": float32(b2cPayload.B2CUtilityAccountAvailableFunds()),
				"mpesa_charges":         float32(b2cPayload.B2CChargesPaidAccountAvailableFunds()),
				"recipient_registered":  b2cPayload.B2CRecipientIsRegisteredCustomer(),
				"mpesa_receipt_id":      b2cPayload.TransactionReceipt(),
				"transaction_time":      sql.NullTime{Valid: true, Time: b2cPayload.TransactionCompletedDateTime().UTC()},
				"receiver_public_name":  b2cPayload.ReceiverPartyPublicName(),
				"b2c_status":            status,
				"succeeded":             succeeded,
			}).Error
		if err != nil {
			return http.StatusInternalServerError, fmt.Errorf("failed to update b2c: %v", err)
		}
	case errors.Is(err, gorm.ErrRecordNotFound):
		// Create B2C
		db = &b2c_app_v1.Payment{
			ID:                         0,
			InitiatorID:                tranferReq.GetInitiatorId(),
			InitiatorCustomerReference: tranferReq.GetInitiatorCustomerReference(),
			InitiatorCustomerNames:     tranferReq.GetInitiatorCustomerNames(),
			Msisdn:                     b2cPayload.MSISDN(),
			OrgShortCode:               tranferReq.ShortCode,
			CommandId:                  tranferReq.CommandId.String(),
			TransactionAmount:          float32(b2cPayload.TransactionAmount()),
			ConversationID:             b2cPayload.ConversationID(),
			OriginatorConversationID:   b2cPayload.OriginatorConversationID(),
			ResponseDescription:        "",
			ResponseCode:               "",
			ResultCode:                 fmt.Sprint(b2cPayload.Result.ResultCode),
			ResultDescription:          b2cPayload.Result.ResultDesc,
			WorkingAccountFunds:        float32(b2cPayload.B2CWorkingAccountAvailableFunds()),
			UtilityAccountFunds:        float32(b2cPayload.B2CUtilityAccountAvailableFunds()),
			MpesaCharges:               float32(b2cPayload.B2CChargesPaidAccountAvailableFunds()),
			SystemCharges:              0,
			RecipientRegistered:        b2cPayload.B2CRecipientIsRegisteredCustomer(),
			MpesaReceiptId: sql.NullString{
				Valid:  b2cPayload.TransactionReceipt() != "",
				String: b2cPayload.TransactionReceipt(),
			},
			ReceiverPublicName: b2cPayload.ReceiverPartyPublicName(),
			B2CStatus:          status,
			Source:             "",
			Tag:                "",
			Succeeded:          succeeded,
			Processed:          "NO",
			TransactionTime:    sql.NullTime{Valid: true, Time: b2cPayload.TransactionCompletedDateTime().UTC()},
			CreatedAt:          time.Time{},
		}
		err = gw.SQLDB.Create(db).Error
		if err != nil {
			return http.StatusInternalServerError, fmt.Errorf("failed to create b2c b2cPayload: %v", err)
		}
	default:
		gw.Logger.Errorln(err)
		return http.StatusInternalServerError, errors.New("failed to create b2c b2cPayload")
	}

	pb, err := b2c_app_v1.PaymentProto(db)
	if err != nil {
		gw.Logger.Errorln(err)
		return http.StatusInternalServerError, errors.New("failed to get b2c proto")
	}

	// Publish the transaction
	if tranferReq.Publish {
		publish := func() {
			_, err = gw.B2CV1API.PublishB2CPayment(gw.ctxExt, &b2c_v1.PublishB2CPaymentRequest{
				PublishMessage: &b2c_v1.PublishMessage{
					InitiatorId:    tranferReq.InitiatorId,
					TransactionId:  pb.TransactionId,
					MpesaReceiptId: pb.MpesaReceiptId,
					Msisdn:         b2cPayload.MSISDN(),
					PublishInfo:    tranferReq.PublishMessage,
					Payment:        pb,
				},
			})
			if err != nil {
				gw.Logger.Warningf("failed to publish message: %v", err)
			} else {
				gw.Logger.Infoln("B2C has been published on channel ", tranferReq.GetPublishMessage().GetChannelName())
			}
		}
		if tranferReq.GetPublishMessage().GetOnlyOnSuccess() {
			if pb.Succeeded {
				publish()
			}
		} else {
			publish()
		}
	}

	_, err = w.Write([]byte("mpesa b2c b2cPayload processed"))
	if err != nil {
		return http.StatusInternalServerError, err
	}

	return http.StatusOK, nil
}

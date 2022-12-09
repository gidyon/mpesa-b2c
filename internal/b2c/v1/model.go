package b2c_app_v1

import (
	"database/sql"
	"fmt"
	"time"

	b2c "github.com/gidyon/mpesa-b2c/pkg/api/b2c/v1"
	"github.com/spf13/viper"
	"gorm.io/gorm"
)

// B2CTable is table name for b2c transactions
const B2CTable = "b2c_transactions"

// Payment is B2C payment model
type Payment struct {
	ID                         uint   `gorm:"primaryKey;autoIncrement"`
	InitiatorID                string `gorm:"index;type:varchar(50)"`
	InitiatorCustomerReference string `gorm:"index;type:varchar(50)"`
	InitiatorCustomerNames     string `gorm:"type:varchar(50)"`

	Msisdn            string  `gorm:"index;type:varchar(15)"`
	OrgShortCode      string  `gorm:"index;type:varchar(15)"`
	CommandId         string  `gorm:"index;type:varchar(30)"`
	TransactionAmount float32 `gorm:"index;type:float(10)"`

	ConversationID           string `gorm:"index;type:varchar(50);not null"`
	OriginatorConversationID string `gorm:"index;type:varchar(50);not null"`
	ResponseDescription      string `gorm:"type:varchar(300)"`
	ResponseCode             string `gorm:"index;type:varchar(10)"`
	ResultCode               string `gorm:"index;type:varchar(10)"`
	ResultDescription        string `gorm:"type:varchar(300)"`

	WorkingAccountFunds float32        `gorm:"type:float(10)"`
	UtilityAccountFunds float32        `gorm:"type:float(10)"`
	MpesaCharges        float32        `gorm:"type:float(10)"`
	SystemCharges       float32        `gorm:"type:float(10)"`
	RecipientRegistered bool           `gorm:"index;type:tinyint(1)"`
	MpesaReceiptId      sql.NullString `gorm:"index;type:varchar(50);unique"`
	ReceiverPublicName  string         `gorm:"type:varchar(50)"`

	B2CStatus string `gorm:"index;type:varchar(30);column:b2c_status"`
	Source    string `gorm:"index;type:varchar(30)"`
	Tag       string `gorm:"index;type:varchar(30)"`
	Succeeded string `gorm:"index;type:enum('YES','NO', 'UNKNOWN');default:NO"`
	Processed string `gorm:"index;type:enum('YES','NO');default:NO"`

	TransactionTime sql.NullTime `gorm:"index;type:datetime(6)"`
	UpdatedAt       time.Time    `gorm:"autoUpdateTime;type:datetime(6)"`
	CreatedAt       time.Time    `gorm:"index;autoCreateTime;type:datetime(6);not null"`
}

// TableName is table name for model
func (*Payment) TableName() string {
	// Get table prefix
	if viper.GetString("B2C_TABLE_PREFIX") != "" {
		return fmt.Sprintf("%s_%s", viper.GetString("B2C_TABLE_PREFIX"), B2CTable)
	}
	return B2CTable
}

// DailyStat contains statistics for a day
type DailyStat struct {
	ID                     uint   `gorm:"primaryKey;autoIncrement"`
	OrgShortCode           string `gorm:"index;type:varchar(20);not null"`
	Date                   string `gorm:"index;type:varchar(10);not null"`
	TotalTransactions      int32  `gorm:"type:int(10);not null"`
	SuccessfulTransactions int32
	FailedTransactions     int32
	TotalAmountTransacted  float32        `gorm:"index;type:float(15)"`
	TotalCharges           float32        `gorm:"index;type:float(15)"`
	CreatedAt              time.Time      `gorm:"autoCreateTime"`
	UpdatedAt              time.Time      `gorm:"autoCreateTime"`
	DeletedAt              gorm.DeletedAt `gorm:"index"`
}

const statsTable = "b2c_daily_stats"

// TableName ...
func (*DailyStat) TableName() string {
	if viper.GetString("B2C_TABLE_PREFIX") != "" {
		return fmt.Sprintf("%s_%s", viper.GetString("B2C_TABLE_PREFIX"), statsTable)
	}
	return statsTable
}

func PaymentProto(db *Payment) (*b2c.B2CPayment, error) {
	pb := &b2c.B2CPayment{
		TransactionId:              uint64(db.ID),
		InitiatorId:                db.InitiatorID,
		InitiatorCustomerReference: db.InitiatorCustomerReference,
		InitiatorCustomerNames:     db.InitiatorCustomerNames,
		OrgShortCode:               db.OrgShortCode,
		CommandId:                  b2c.CommandId(b2c.CommandId_value[db.CommandId]),
		Msisdn:                     db.Msisdn,
		Amount:                     db.TransactionAmount,
		ConversationId:             db.ConversationID,
		OriginalConversationId:     db.OriginatorConversationID,
		B2CResponseDescription:     db.ResponseDescription,
		B2CResponseCode:            db.ResponseCode,
		B2CResultDescription:       db.ResultDescription,
		B2CResultCode:              db.ResultCode,
		ReceiverPartyPublicName:    db.ReceiverPublicName,
		MpesaReceiptId:             db.MpesaReceiptId.String,
		WorkingAccountFunds:        db.WorkingAccountFunds,
		UtilityAccountFunds:        db.UtilityAccountFunds,
		MpesaCharges:               db.MpesaCharges,
		SystemCharges:              db.SystemCharges,
		RecipientRegistered:        db.RecipientRegistered,
		B2CStatus:                  b2c.B2CStatus(b2c.B2CStatus_value[db.B2CStatus]),
		Source:                     db.Source,
		Tag:                        db.Tag,
		Succeeded:                  db.Succeeded == "YES",
		Processed:                  db.Processed == "YES",
		TransactionTimestamp:       db.TransactionTime.Time.UTC().Unix(),
		CreateDate:                 db.CreatedAt.UTC().Format(time.RFC3339),
	}
	return pb, nil
}

// StatModel gets mpesa statistics model from protobuf message
func StatModel(pb *b2c.DailyStat) (*DailyStat, error) {
	return &DailyStat{
		ID:                     0,
		OrgShortCode:           pb.OrgShortCode,
		Date:                   pb.Date,
		TotalTransactions:      pb.TotalTransactions,
		SuccessfulTransactions: int32(pb.SuccessfulTransactions),
		FailedTransactions:     int32(pb.FailedTransactions),
		TotalAmountTransacted:  pb.TotalAmountTransacted,
		TotalCharges:           pb.TotalCharges,
		CreatedAt:              time.Time{},
		UpdatedAt:              time.Time{},
		DeletedAt:              gorm.DeletedAt{},
	}, nil
}

// StatProto gets mpesa statistics protobuf from model
func StatProto(db *DailyStat) (*b2c.DailyStat, error) {
	return &b2c.DailyStat{
		StatId:                 fmt.Sprint(db.ID),
		Date:                   "",
		OrgShortCode:           db.OrgShortCode,
		TotalTransactions:      db.TotalTransactions,
		SuccessfulTransactions: int64(db.SuccessfulTransactions),
		FailedTransactions:     int64(db.FailedTransactions),
		TotalAmountTransacted:  db.TotalAmountTransacted,
		TotalCharges:           db.TotalCharges,
		CreateTimeSeconds:      db.CreatedAt.Unix(),
		UpdateTimeSeconds:      db.UpdatedAt.Unix(),
	}, nil
}

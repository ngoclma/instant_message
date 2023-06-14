package main

import (
	"context"
	"database/sql"
	"math/rand"

	"github.com/TikTokTechImmersion/assignment_demo_2023/rpc-server/kitex_gen/rpc"
	_ "github.com/go-sql-driver/mysql"
)

// IMServiceImpl implements the last service interface defined in the IDL.
type IMServiceImpl struct{}

func (s *IMServiceImpl) Send(ctx context.Context, req *rpc.SendRequest) (*rpc.SendResponse, error) {
	resp := rpc.NewSendResponse()
	resp.Code, resp.Msg = saveMessage(req.Message)
	return resp, nil
}

func (s *IMServiceImpl) Pull(ctx context.Context, req *rpc.PullRequest) (*rpc.PullResponse, error) {
	resp := rpc.NewPullResponse()
	resp.Code, resp.Msg = getMessage(req, resp)
	return resp, nil
}

func areYouLucky() (int32, string) {
	if rand.Int31n(2) == 1 {
		return 0, "success"
	} else {
		return 500, "oops"
	}
}

func saveMessage(msg *rpc.Message) (int32, string) {
	db, err := sql.Open("mysql", "root:example@tcp(docker.for.mac.localhost:3306)/msgdb")
	if err != nil {
		return 500, err.Error()
	}
	ins, err := db.Prepare("INSERT INTO messages (chat, sender, msg, send_time) VALUES (?, ?, ?, ?)")
	defer ins.Close()
	_, err = ins.Query(msg.Chat, msg.Text, msg.Sender, msg.SendTime)
	if err != nil {
		// log.Printf("Error %s when opening DB\n", err)
		return 500, err.Error()
	}
	return 0, "success"
}

func getMessage(msg *rpc.PullRequest, resp *rpc.PullResponse) (int32, string) {
	db, err := sql.Open("mysql", "root:example@tcp(docker.for.mac.localhost:3306)/msgdb")
	if err != nil {
		return 500, err.Error()
	}
	var ins *sql.Stmt
	if *(msg.Reverse) == true {
		ins, err = db.Prepare("SELECT * FROM messages WHERE chat = ? AND send_time >= ? ORDER BY send_time DESC limit ?")
	} else {
		ins, err = db.Prepare("SELECT * FROM messages WHERE chat = ? AND send_time <= ? ORDER BY send_time ASC limit ?")
	}
	defer ins.Close()
	q, err := ins.Query(msg.Chat, msg.Cursor, msg.Limit)
	if err != nil {
		// log.Printf("Error %s when opening DB\n", err)
		return 500, err.Error()
	}
	for q.Next() {
		var res_chat string
		var res_sender string
		var res_txt string
		var res_send_time int64
		q.Scan(&res_chat, &res_sender, &res_txt, &res_send_time)
		res_msg := rpc.NewMessage()
		res_msg.SetChat(res_chat)
		res_msg.SetSender(res_sender)
		res_msg.SetText(res_txt)
		res_msg.SetSendTime(res_send_time)
		resp.Messages = append(resp.Messages, res_msg)
	}
	return 0, "success"
}

// func saveMessage(id string, sender string, msg string, sendtime int64) (int32, string) {
// 	db, err := sql.Open("mysql", "root:example@tcp(docker.for.mac.localhost:3306)/msgdb")
// 	if err != nil {
// 		panic(err)
// 	}
// 	insert, err := db.Prepare("INSERT INTO messages VALUES (?, ?, ?, ?)")
// 	defer insert.Close()
// 	_, err = insert.Query(id, sender, msg, sendtime)
// 	if err != nil {
// 		return 500, err.Error() + fmt.Sprintf("INSERT INTO messages VALUES ('%s', '%s', '%s', %d)", id, sender, msg, sendtime)
// 	}
// 	return 0, "Success"
// }

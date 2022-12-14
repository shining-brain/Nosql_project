package main

import (
	"NoSql_project/client"
	"NoSql_project/pool"
	"NoSql_project/utils"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/redis/protocol"
	"html/template"
	"net/http"
	"strconv"
	"time"
)

var client_pool *pool.Pool

type show_one_response struct {
	Member string
	Score  string
}

type show_one_rank_response struct {
	Member string
	Rank   string
}

type show_rank_response struct {
	Id     string
	Member string
	Score  string
}

func main() {
	http.HandleFunc("/", login)
	http.HandleFunc("/test", test)
	http.HandleFunc("/show_rank", show_rank)
	http.HandleFunc("/preheating", preheating)
	http.HandleFunc("/new_item", new_item)
	http.HandleFunc("/back", back)
	http.HandleFunc("/fix", fix)
	http.HandleFunc("/show_one", show_one)
	http.HandleFunc("/delete_one", delete_one)
	http.HandleFunc("/one_rank", one_rank)
	http.ListenAndServe(":8080", nil)
}

//查询单个member的rank
func one_rank(writer http.ResponseWriter, request *http.Request) {
	request.ParseForm()
	member := request.FormValue("member")
	key := request.FormValue("key")
	logger.Setup(&logger.Settings{
		Path:       "logs",
		Name:       "godis",
		Ext:        ".log",
		TimeFormat: "2006-01-02",
	})
	//client, err := client.MakeClient("localhost:6399")
	//if err != nil {
	//	panic(err)
	//}
	//client.Start()

	//采用池化技术优化连接
	rawClient, _ := client_pool.Get()
	defer client_pool.Put(rawClient)
	client := rawClient.(*client.Client)
	raw := client.Send([][]byte{
		[]byte("ZRevRank"),
		[]byte(key),
		[]byte(member),
	})
	reply, ok := raw.(*protocol.IntReply)
	if !ok {
		logger.Info("单个member的排名查询失败：", key, member)
		t, _ := template.ParseFiles("src/ikinai.html")
		t.Execute(writer, nil)
		return
	}

	var response show_one_rank_response
	response.Member = member
	response.Rank = strconv.FormatInt(reply.Code+1, 10)
	logger.Info("单个member的排名查询成功：", key, string(reply.Code+1))
	t, _ := template.ParseFiles("src/one_rank.html")
	t.Execute(writer, response)
	return
}

//删除一个已有的帖子
func delete_one(writer http.ResponseWriter, request *http.Request) {
	request.ParseForm()
	member := request.FormValue("member")
	key := request.FormValue("key")
	logger.Setup(&logger.Settings{
		Path:       "logs",
		Name:       "godis",
		Ext:        ".log",
		TimeFormat: "2006-01-02",
	})
	//client, err := client.MakeClient("localhost:6399")
	//if err != nil {
	//	panic(err)
	//}
	//client.Start()
	//defer client.Close()

	//采用池化技术优化连接
	rawClient, _ := client_pool.Get()
	defer client_pool.Put(rawClient)
	client := rawClient.(*client.Client)

	result := client.Send([][]byte{
		[]byte("ZScore"),
		[]byte(key),
		[]byte(member),
	})
	if _, ok := result.(*protocol.BulkReply); !ok {
		logger.Info("数据还不存在")
		t, _ := template.ParseFiles("src/unexist.html")
		t.Execute(writer, nil)
		return
	}

	result = client.Send([][]byte{
		[]byte("ZRem"),
		[]byte(key),
		[]byte(member),
	})
	reply, ok := result.(*protocol.IntReply)
	if !ok {
		logger.Info("数据删除失败：", key, member)
		t, _ := template.ParseFiles("src/ikinai.html")
		t.Execute(writer, nil)
		return
	}
	logger.Info("成功删除数据条数：", reply.Code)
	t, _ := template.ParseFiles("src/deleted.html")
	t.Execute(writer, nil)
	return
}

//查询单个指定帖子的热度
func show_one(writer http.ResponseWriter, request *http.Request) {
	request.ParseForm()
	member := request.FormValue("member")
	key := request.FormValue("key")

	logger.Setup(&logger.Settings{
		Path:       "logs",
		Name:       "godis",
		Ext:        ".log",
		TimeFormat: "2006-01-02",
	})
	//client, err := client.MakeClient("localhost:6399")
	//if err != nil {
	//	panic(err)
	//}
	//client.Start()
	//defer client.Close()

	//采用池化技术优化连接
	rawClient, _ := client_pool.Get()
	defer client_pool.Put(rawClient)
	client := rawClient.(*client.Client)
	result := client.Send([][]byte{
		[]byte("ZScore"),
		[]byte(key),
		[]byte(member),
	})
	reply, ok := result.(*protocol.BulkReply)
	if !ok {
		logger.Info("数据还不存在")
		t, _ := template.ParseFiles("src/unexist.html")
		t.Execute(writer, nil)
		return
	}
	logger.Info("热度查询成功：", string(reply.Arg))
	var response show_one_response
	response.Score = string(reply.Arg)
	response.Member = member
	t, _ := template.ParseFiles("src/show_one.html")
	t.Execute(writer, response)

}

//修改制定项目的热度
func fix(writer http.ResponseWriter, request *http.Request) {
	request.ParseForm()
	member := request.FormValue("member")
	score := request.FormValue("score")
	key := request.FormValue("key")
	if score == "" {
		score = "0"
	}

	logger.Setup(&logger.Settings{
		Path:       "logs",
		Name:       "godis",
		Ext:        ".log",
		TimeFormat: "2006-01-02",
	})
	//client, err := client.MakeClient("localhost:6399")
	//if err != nil {
	//	panic(err)
	//}
	//client.Start()
	//defer client.Close()

	//采用池化技术优化连接
	rawClient, _ := client_pool.Get()
	defer client_pool.Put(rawClient)
	client := rawClient.(*client.Client)
	result := client.Send([][]byte{
		[]byte("ZScore"),
		[]byte(key),
		[]byte(member),
	})
	if _, ok := result.(*protocol.BulkReply); !ok {
		logger.Info("数据还不存在")
		t, _ := template.ParseFiles("src/unexist.html")
		t.Execute(writer, nil)
		return
	}

	result = client.Send([][]byte{
		[]byte("ZAdd"),
		[]byte(key),
		[]byte(score),
		[]byte(member),
	})
	reply, ok := result.(*protocol.IntReply)
	if !ok {
		logger.Info("数据修改失败：", key, score, member)
		t, _ := template.ParseFiles("src/ikinai.html")
		t.Execute(writer, nil)
		return
	}
	logger.Info("数据修改成功，修改项目数：", reply.Code)
	t, _ := template.ParseFiles("src/pass.html")
	t.Execute(writer, nil)
	return
}

//返回main页面
func back(writer http.ResponseWriter, request *http.Request) {
	request.ParseForm()
	writer.Header().Set("Location", "/")
	writer.WriteHeader(302)
}

//插入新的热搜
func new_item(writer http.ResponseWriter, request *http.Request) {
	request.ParseForm()
	member := request.FormValue("member")
	score := request.FormValue("score")
	key := request.FormValue("key")
	if score == "" {
		score = "0"
	}

	logger.Setup(&logger.Settings{
		Path:       "logs",
		Name:       "godis",
		Ext:        ".log",
		TimeFormat: "2006-01-02",
	})
	//client, err := client.MakeClient("localhost:6399")
	//if err != nil {
	//	panic(err)
	//}
	//client.Start()
	//defer client.Close()

	//采用池化技术优化连接
	rawClient, _ := client_pool.Get()
	defer client_pool.Put(rawClient)
	client := rawClient.(*client.Client)
	result := client.Send([][]byte{
		[]byte("ZScore"),
		[]byte(key),
		[]byte(member),
	})
	if _, ok := result.(*protocol.BulkReply); ok {
		logger.Info("数据已存在")
		t, _ := template.ParseFiles("src/exist.html")
		t.Execute(writer, nil)
		return
	}

	result = client.Send([][]byte{
		[]byte("ZAdd"),
		[]byte(key),
		[]byte(score),
		[]byte(member),
	})
	reply, ok := result.(*protocol.IntReply)
	if !ok {
		logger.Info("数据插入失败：", key, score, member)
		t, _ := template.ParseFiles("src/ikinai.html")
		t.Execute(writer, nil)
		return
	}
	logger.Info("数据插入成功，添加项目数：", reply.Code)
	t, _ := template.ParseFiles("src/pass.html")
	t.Execute(writer, nil)
	return
}

//用于Godis数据库的预热
func preheating(writer http.ResponseWriter, request *http.Request) {
	logger.Setup(&logger.Settings{
		Path:       "logs",
		Name:       "godis",
		Ext:        ".log",
		TimeFormat: "2006-01-02",
	})
	//client, err := client.MakeClient("localhost:6399")
	//if err != nil {
	//	panic(err)
	//}
	//client.Start()
	//defer client.Close()

	//采用池化技术优化连接
	rawClient, _ := client_pool.Get()
	defer client_pool.Put(rawClient)
	client := rawClient.(*client.Client)
	start := time.Now()
	for i := 0; i < 100000; i++ {
		client.Send([][]byte{
			[]byte("ZAdd"),
			[]byte("test1"),
			[]byte(strconv.Itoa(utils.RandInt(1000))),
			[]byte(utils.RandomString(16)),
		})

	}
	use := time.Since(start)
	logger.Info("预热数据用时：", use)
}

//查询最热门的50条帖子
func show_rank(writer http.ResponseWriter, request *http.Request) {
	request.ParseForm()
	key := request.FormValue("key")
	logger.Setup(&logger.Settings{
		Path:       "logs",
		Name:       "godis",
		Ext:        ".log",
		TimeFormat: "2006-01-02",
	})
	//client, err := client.MakeClient("localhost:6399")
	//if err != nil {
	//	panic(err)
	//}
	//client.Start()
	//defer client.Close()

	//采用池化技术优化连接
	rawClient, _ := client_pool.Get()
	defer client_pool.Put(rawClient)
	client := rawClient.(*client.Client)
	result := client.Send([][]byte{
		[]byte("ZRevRange"),
		[]byte(key),
		[]byte("0"),
		[]byte("49"),
		[]byte("WITHSCORES"),
	})

	raw, ok := result.(*protocol.MultiBulkReply)
	if !ok {
		logger.Info("热榜查询失败：", key)
		t, _ := template.ParseFiles("src/ikinai.html")
		t.Execute(writer, nil)
		return
	}
	var response [50]show_rank_response
	reply := raw.Args
	for i := 0; i < len(reply)/2; i++ {
		response[i].Id = strconv.Itoa(i + 1)
		response[i].Member = string(reply[2*i])
		response[i].Score = string(reply[2*i+1])
	}
	logger.Info("热榜查询成功，数据装载成功")
	t, _ := template.ParseFiles("src/show_rank.html")
	t.Execute(writer, response)
	return
}

//测试路由
func test(writer http.ResponseWriter, request *http.Request) {
	logger.Setup(&logger.Settings{
		Path:       "logs",
		Name:       "godis",
		Ext:        ".log",
		TimeFormat: "2006-01-02",
	})
	//client, err := client.MakeClient("localhost:6399")
	//if err != nil {
	//	panic(err)
	//}
	//client.Start()
	//defer client.Close()

	//采用池化技术优化连接
	rawClient, _ := client_pool.Get()
	defer client_pool.Put(rawClient)
	client := rawClient.(*client.Client)
	result := client.Send([][]byte{
		[]byte("PING"),
	})
	if statusRet, ok := result.(*protocol.StatusReply); ok {
		if statusRet.Status != "PONG" {
			panic("ping失败")
		}
	}
	logger.Info("ping通了！")
}

//初始化客户端，开启连接池
func login(writer http.ResponseWriter, request *http.Request) {
	request.ParseForm()
	if request.Method == "GET" {
		t, _ := template.ParseFiles("src/main.html")
		t.Execute(writer, nil)
	}

	//已经存在连接池时，则直接返回
	if client_pool != nil {
		return
	}
	factory := func() (interface{}, error) {
		makeClient, err := client.MakeClient("localhost:6399")
		if err != nil {
			panic(err)
		}
		makeClient.Start()
		return makeClient, nil
	}
	finalizer := func(raw interface{}) {
		client := raw.(*client.Client)
		client.Close()
	}
	cfg := pool.Config{
		MaxIdle:   20,
		MaxActive: 40,
	}
	client_pool = pool.New(factory, finalizer, cfg)

	logger.Info("连接池已准备就绪，本线程池的最大连接数为：", cfg.MaxActive)
}

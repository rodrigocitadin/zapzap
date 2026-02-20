package main

import (
	"context"
	"embed"
	"html/template"
	"io"
	"io/fs"
	"net/http"

	"github.com/0x6flab/namegenerator"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/labstack/echo/v5"
	"github.com/labstack/echo/v5/middleware"
)

type Client struct {
	ClientID   uuid.UUID    `json:"sender_id"`
	ClientName string       `json:"sender_name"`
	Ch         chan Message `json:"-"`
}

type Message struct {
	Message string `json:"message"`
	Sender  Client `json:"sender"`
}

type Hub struct {
	clients    map[uuid.UUID]*Client
	register   chan *Client
	unregister chan *Client
	broadcast  chan Message
}

func (h *Hub) Run() {
	for {
		select {
		case client := <-h.register:
			h.clients[client.ClientID] = client

		case client := <-h.unregister:
			delete(h.clients, client.ClientID)

		case msg := <-h.broadcast:
			for _, client := range h.clients {
				select {
				case client.Ch <- msg:
				default:
					// drop if slow
				}
			}
		}
	}
}

var (
	generator = namegenerator.NewGenerator()
	hub       = Hub{
		clients:    make(map[uuid.UUID]*Client),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		broadcast:  make(chan Message),
	}
	upgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}
)

func chat(c *echo.Context) error {
	ws, err := upgrader.Upgrade(c.Response(), c.Request(), nil)
	if err != nil {
		return err
	}

	client := &Client{
		ClientID:   uuid.New(),
		ClientName: generator.Generate(),
		Ch:         make(chan Message, 10),
	}

	hub.register <- client
	defer func() {
		hub.unregister <- client
		close(client.Ch)
		ws.Close()
	}()

	go writer(ws, client)
	reader(ws, client)

	return nil
}

func writer(ws *websocket.Conn, client *Client) {
	for msg := range client.Ch {
		if err := ws.WriteJSON(msg); err != nil {
			return
		}
	}
}

func reader(ws *websocket.Conn, client *Client) {
	for {
		var msg Message
		if err := ws.ReadJSON(&msg); err != nil {
			return
		}

		msg.Sender = *client
		hub.broadcast <- msg
	}
}

func index(c *echo.Context) error {
	return c.Render(http.StatusOK, "index.html", nil)
}

type Template struct {
	templates *template.Template
}

func (t *Template) Render(
	ctx *echo.Context,
	w io.Writer,
	name string,
	data any,
) error {
	return t.templates.ExecuteTemplate(w, name, data)
}

//go:embed templates/*.html
var templateFS embed.FS

//go:embed static/*
var staticFS embed.FS

func main() {
	e := echo.New()

	e.Use(middleware.RequestLogger())
	e.Use(middleware.Recover())
	e.Use(middleware.Gzip())

	t := &Template{
		templates: template.Must(template.ParseFS(templateFS, "templates/*.html")),
	}
	e.Renderer = t

	staticSub, _ := fs.Sub(staticFS, "static")
	e.StaticFS("/static", staticSub)

	e.GET("/", index)
	e.GET("/ws", chat)

	go hub.Run()

	sc := echo.StartConfig{Address: ":1323"}
	if err := sc.Start(context.Background(), e); err != nil {
		e.Logger.Error("failed to start server", "error", err)
	}
}

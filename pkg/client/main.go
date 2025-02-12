package client

import (
    "context"
    "time"
    "google.golang.org/grpc"
)

type Client struct {
    servers     []string
    leader      string
    conn        *grpc.ClientConn
    client      pb.KVStoreClient
    connTimeout time.Duration
}

func NewClient(servers []string) (*Client, error) {
    return &Client{
        servers:     servers,
        connTimeout: 5 * time.Second,
    }
}

func (c *Client) Get(key string) (string, error) {
    ctx, cancel := context.WithTimeout(context.Background(), c.connTimeout)
    defer cancel()
    
    resp, err := c.client.Get(ctx, &pb.GetRequest{Key: key})
    if err != nil {
        return "", err
    }
    
    if !resp.Exists {
        return "", nil
    }
    
    return resp.Value, nil
}
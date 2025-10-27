package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

type Options = goredis.Options

type Parser interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
}

type JSONParser struct{}

func (j JSONParser) Marshal(v any) ([]byte, error) {
	return json.Marshal(v)
}

func (j JSONParser) Unmarshal(data []byte, v any) error {
	return json.Unmarshal(data, v)
}

type Redis struct {
	client *goredis.Client
	ctx    context.Context
	parser Parser
}

func New(ctx context.Context, opts *Options) *Redis {
	return &Redis{
		client: goredis.NewClient(opts),
		ctx:    ctx,
		parser: JSONParser{},
	}
}

func NewDefault(ctx context.Context, addr, password string, db int) *Redis {
	return &Redis{
		client: goredis.NewClient(&goredis.Options{
			Addr:     addr,
			Password: password,
			DB:       db,
		}),
		ctx:    ctx,
		parser: JSONParser{},
	}
}

// Ping test connection
func (r *Redis) Ping() error {
	if r.client == nil {
		return fmt.Errorf("redis client not initialized")
	}
	return r.client.Ping(r.ctx).Err()
}

func (r *Redis) Close() error {
	if r.client == nil {
		return nil
	}
	return r.client.Close()
}

func (r *Redis) SetParser(p Parser) {
	r.parser = p
}

func (r *Redis) SetJSON(key string, value any, exp time.Duration) error {
	b, err := r.parser.Marshal(value)
	if err != nil {
		return err
	}
	return r.Set(key, b, exp)
}

func (r *Redis) GetJSON(key string, out any) error {
	val, err := r.Get(key)
	if err != nil {
		return err
	}
	return r.parser.Unmarshal([]byte(val), out)
}

// Set sets key-value with expiration
func (c *Redis) Set(key string, value any, exp time.Duration) error {
	return c.client.Set(c.ctx, key, value, exp).Err()
}

// Get gets value by key
func (r *Redis) Get(key string) (string, error) {
	return r.client.Get(r.ctx, key).Result()
}

// Del deletes a key
func (r *Redis) Del(key string) error {
	return r.client.Del(r.ctx, key).Err()
}

// Exists checks if a key exists
func (r *Redis) Exists(key string) (bool, error) {
	n, err := r.client.Exists(r.ctx, key).Result()
	return n > 0, err
}

// Expire sets expiration for a key
func (r *Redis) Expire(key string, exp time.Duration) (bool, error) {
	return r.client.Expire(r.ctx, key, exp).Result()
}

// HSet sets a field in a hash
func (r *Redis) HSet(key, field string, value any) error {
	return r.client.HSet(r.ctx, key, field, value).Err()
}

// HGet gets a field from a hash
func (r *Redis) HGet(key, field string) (string, error) {
	return r.client.HGet(r.ctx, key, field).Result()
}

// HDel deletes a field from a hash
func (r *Redis) HDel(key string, fields ...string) error {
	return r.client.HDel(r.ctx, key, fields...).Err()
}

// HExists checks if a field exists in a hash
func (r *Redis) HExists(key, field string) (bool, error) {
	n, err := r.client.HExists(r.ctx, key, field).Result()
	return n, err
}

// HGetAll gets all fields and values from a hash
func (r *Redis) HGetAll(key string) (map[string]string, error) {
	return r.client.HGetAll(r.ctx, key).Result()
}

// LPop pops a value from the left of the list
func (r *Redis) LPop(key string) (string, error) {
	return r.client.LPop(r.ctx, key).Result()
}

// RPop pops a value from the right of the list
func (r *Redis) RPop(key string) (string, error) {
	return r.client.RPop(r.ctx, key).Result()
}

// LRange gets a range of values from the list
func (r *Redis) LRange(key string, start, stop int64) ([]string, error) {
	return r.client.LRange(r.ctx, key, start, stop).Result()
}

// LPush pushes values to the left of the list
func (r *Redis) LPush(key string, values ...any) error {
	return r.client.LPush(r.ctx, key, values...).Err()
}

// RPush pushes values to the right of the list
func (r *Redis) RPush(key string, values ...any) error {
	return r.client.RPush(r.ctx, key, values...).Err()
}

// SAdd adds members to the set
func (r *Redis) SAdd(key string, members ...any) error {
	return r.client.SAdd(r.ctx, key, members...).Err()
}

// SRem removes members from the set
func (r *Redis) SRem(key string, members ...any) error {
	return r.client.SRem(r.ctx, key, members...).Err()
}

// SMembers gets all members of the set
func (r *Redis) SMembers(key string) ([]string, error) {
	return r.client.SMembers(r.ctx, key).Result()
}

// SIsMember checks if a member is in the set
func (r *Redis) SIsMember(key string, member any) (bool, error) {
	return r.client.SIsMember(r.ctx, key, member).Result()
}

// Keys gets all keys matching the pattern
func (r *Redis) Keys(pattern string) ([]string, error) {
	return r.client.Keys(r.ctx, pattern).Result()
}

// TTL gets the time to live for a key
func (r *Redis) TTL(key string) (time.Duration, error) {
	return r.client.TTL(r.ctx, key).Result()
}

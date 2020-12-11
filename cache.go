package main

import "sync"

type safeCache struct {
	lock  sync.Mutex
	store map[string]interface{}
}

func NewSafeCache() *safeCache {
	return &safeCache{
		store: map[string]interface{}{},
	}
}

// Sync calls the cb function passing the current object in the store for that key in a thread safe manor
// if the cb errors it returns that error
// Sync then stores the result of cb back in the store for that key
func (c *safeCache) Sync(key string, cb func(obj interface{}) (interface{}, error)) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	item := c.store[key]
	cbi, err := cb(item)
	if err != nil {
		return err
	}
	if cbi != item && cbi != nil {
		c.store[key] = cbi
	}
	return nil
}

func (c *safeCache) Get(key string) interface{} {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.store[key]
}

func (c *safeCache) Delete(key string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	delete(c.store, key)
}

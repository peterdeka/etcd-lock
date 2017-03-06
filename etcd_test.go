//this is a dumb test just to check minimal functionality

package etcdlock

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const c1 = "client1"
const c2 = "client2"

func TestLock(t *testing.T) {
	req := require.New(t)
	cli := NewEtcdRegistry([]string{"http://127.0.0.1:4001"})
	m, e := NewMaster(cli, "testlock", c1, 5*time.Second)
	req.NoError(e)
	go func() {
		for {
			r := <-m.EventsChan()
			req.Equal(MasterAdded, r.Type)
			req.Equal(c1, r.Master)
		}
	}()
	m.Start()
	time.Sleep(2 * time.Second)
	m.Stop()
	fmt.Println(m.GetHolder())
}

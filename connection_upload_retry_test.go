package torrent

import (
	"testing"
	"testing/synctest"
	"time"
)

// A channel-backed locker lets synctest observe a blocked Lock call.
type uploadRetryTestLocker chan struct{}

func (l uploadRetryTestLocker) Lock()   { l <- struct{}{} }
func (l uploadRetryTestLocker) Unlock() { <-l }

func TestUploadRetryWakesWriter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		locker := make(uploadRetryTestLocker, 1)
		c := &connection{}
		c.writerCond.L = locker
		enterWait := make(chan struct{})
		woke := make(chan struct{})
		const retryDelay = time.Millisecond

		go func() {
			locker.Lock()
			defer locker.Unlock()
			c.setRetryUploadTimer(retryDelay)
			// Model the writer after its empty-buffer check, just before Wait.
			<-enterWait
			c.writerCond.Wait()
			close(woke)
		}()

		synctest.Wait()
		<-time.After(retryDelay + time.Nanosecond)
		// The callback has either run or is blocked on the writer's lock.
		synctest.Wait()
		close(enterWait)
		synctest.Wait()

		select {
		case <-woke:
		default:
			// Release the waiter before reporting the missed wakeup.
			locker.Lock()
			c.writerCond.Broadcast()
			locker.Unlock()
			t.Fatal("upload retry expired before Wait and failed to wake the writer")
		}
	})
}

package chunk

import (
	"context"
	io "io"
	"path"
	"strings"
	"time"

	"github.com/pachyderm/pachyderm/src/client/pkg/errors"
	"github.com/pachyderm/pachyderm/src/server/pkg/obj"
	"github.com/pachyderm/pachyderm/src/server/pkg/storage/track"
)

// Client allows manipulation of individual chunks, by maintaining consistency between
// a tracker and an obj.Client.
type Client struct {
	objc    obj.Client
	mdstore MetadataStore
	tracker track.Tracker
	renewer *track.Renewer
	ttl     time.Duration
}

func NewClient(objc obj.Client, mdstore MetadataStore, tr track.Tracker, chunkSet string) *Client {
	var renewer *track.Renewer
	if chunkSet != "" {
		renewer = track.NewRenewer(tr, chunkSet, defaultChunkTTL)
	}
	c := &Client{
		objc:    objc,
		tracker: tr,
		mdstore: mdstore,
		renewer: renewer,
		ttl:     defaultChunkTTL,
	}
	return c
}

func (c *Client) Create(ctx context.Context, md Metadata, chunkData []byte) (ID, error) {	
	chunkID := Hash(chunkData)
	var pointsTo []string
	for _, cid := range md.PointsTo {
		pointsTo = append(pointsTo, ChunkObjectID(cid))
	}
	// TODO: retry on ErrTombstone
	chunkOID := ChunkObjectID(chunkID)
	if err := c.tracker.CreateObject(ctx, chunkOID, pointsTo, c.ttl); err != nil {
		if err != track.ErrObjectExists {
			return nil, err
		}
	}
	if err := c.renewer.Add(ctx, chunkOID); err != nil {
		return nil, err
	}
	// at this point no one will be trying to delete the chunk, because there is an object pointing to it.
	p := chunkPath(chunkID)
	if c.objc.Exists(ctx, p) {
		return chunkID, nil
	}
	if err := c.mdstore.Set(ctx, chunkID, md); err != nil {
		return nil, err
	}
	objW, err := c.objc.Writer(ctx, p)
	if err != nil {
		return nil, err
	}
	if _, err = objW.Write(chunkData); err != nil {
		return nil, err
	}
	if err := objW.Close(); err != nil {
		return nil, err
	}
	return chunkID, nil
}

func (c *Client) Get(ctx context.Context, chunkID ID, w io.Writer) error {
	p := chunkPath(chunkID)
	objR, err := c.objc.Reader(ctx, p, 0, 0)
	if err != nil {
		return err
	}
	defer objR.Close()
	_, err = io.Copy(w, objR)
	return err
}

func (c *Client) Close() error {
	return c.renewer.Close()
}

func chunkPath(chunkID ID) string {
	if len(chunkID) == 0 {
		panic("chunkID cannot be empty")
	}
	return path.Join(prefix, chunkID.HexString())
}

func ChunkObjectID(chunkID ID) string {
	return "chunk/" + chunkID.HexString()
}

var _ track.Deleter = &deleter{}

type deleter struct {
	mdstore MetadataStore
	objc    obj.Client
}

func (d *deleter) Delete(ctx context.Context, id string) error {
	const prefix = "chunks/"
	if !strings.HasPrefix(id, prefix) {
		return errors.Errorf("cannot delete (%s)", id)
	}
	chunkID, err := ChunkIDFromHex(id[len(prefix):])
	if err != nil {
		return err
	}
	if err := d.objc.Delete(ctx, chunkPath(chunkID)); err != nil {
		return err
	}
	return d.mdstore.Delete(ctx, chunkID)
}

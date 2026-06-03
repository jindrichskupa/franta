package decode

import (
	"bytes"
	"compress/zlib"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"

	"franta/internal/record"
)

// uuidFromBytes formats 16 raw bytes as a canonical 8-4-4-4-12 UUID string.
func uuidFromBytes(b []byte) string {
	if len(b) != 16 {
		return ""
	}
	return hex.EncodeToString(b[0:4]) + "-" +
		hex.EncodeToString(b[4:6]) + "-" +
		hex.EncodeToString(b[6:8]) + "-" +
		hex.EncodeToString(b[8:10]) + "-" +
		hex.EncodeToString(b[10:16])
}

// parseGlueHeader validates the AWS Glue wire-format header and returns the
// compression byte, the schema UUID (canonical string), and the remaining
// payload bytes.
//
// Layout: [0x03][compression][16-byte UUID][payload...]
func parseGlueHeader(b []byte) (compression byte, uuid string, body []byte, err error) {
	if len(b) < 18 {
		return 0, "", nil, fmt.Errorf("glue payload too short: %d bytes", len(b))
	}
	if b[0] != 0x03 {
		return 0, "", nil, fmt.Errorf("glue header version %#x, want 0x03", b[0])
	}
	return b[1], uuidFromBytes(b[2:18]), b[18:], nil
}

// decompressIfZlib returns body as-is for compression byte 0x00, zlib-
// decompresses for 0x05, or errors for any other byte.
func decompressIfZlib(compression byte, body []byte) ([]byte, error) {
	switch compression {
	case 0x00:
		return body, nil
	case 0x05:
		r, err := zlib.NewReader(bytes.NewReader(body))
		if err != nil {
			return nil, fmt.Errorf("zlib reader: %w", err)
		}
		defer func() { _ = r.Close() }()
		out, err := io.ReadAll(r)
		if err != nil {
			return nil, fmt.Errorf("zlib decompress: %w", err)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unknown glue compression byte %#x", compression)
	}
}

// GlueRegistry decodes Glue-wire-format records using AWS Glue Schema Registry.
type GlueRegistry struct {
	client       *glue.Client
	registryName string

	mu    sync.Mutex
	cache map[string]cachedSchema // UUID -> entry
}

// NewGlue builds a GlueRegistry. region/profile/endpoint are optional. An
// empty registryName defaults to "default". The constructor verifies
// connectivity with GetRegistry so misconfig fails fast.
func NewGlue(region, profile, registryName, endpoint string) (*GlueRegistry, error) {
	if registryName == "" {
		registryName = "default"
	}
	// Separate budgets: LoadDefaultConfig may probe IMDS/SSO/profile files for
	// several seconds. Sharing one 5s budget with the GetRegistry ping leaves
	// the ping with a tiny residual deadline that surfaces as a misleading
	// "registry unreachable" error.
	cfgCtx, cfgCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cfgCancel()
	var loadOpts []func(*awsconfig.LoadOptions) error
	if region != "" {
		loadOpts = append(loadOpts, awsconfig.WithRegion(region))
	}
	if profile != "" {
		loadOpts = append(loadOpts, awsconfig.WithSharedConfigProfile(profile))
	}
	cfg, err := awsconfig.LoadDefaultConfig(cfgCtx, loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("aws config: %w", err)
	}
	var glueOpts []func(*glue.Options)
	if endpoint != "" {
		ep := endpoint
		glueOpts = append(glueOpts, func(o *glue.Options) { o.BaseEndpoint = aws.String(ep) })
	}
	cl := glue.NewFromConfig(cfg, glueOpts...)

	pingCtx, pingCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer pingCancel()
	if _, err := cl.GetRegistry(pingCtx, &glue.GetRegistryInput{
		RegistryId: &gluetypes.RegistryId{RegistryName: aws.String(registryName)},
	}); err != nil {
		return nil, fmt.Errorf("glue registry %q unreachable: %w", registryName, err)
	}
	return &GlueRegistry{
		client:       cl,
		registryName: registryName,
		cache:        map[string]cachedSchema{},
	}, nil
}

// Decode renders bytes for display, decoding via Glue when the Glue wire format
// is detected. Failures render placeholders rather than feeding header bytes to
// record.Display.
func (r *GlueRegistry) Decode(b []byte) string {
	compression, uuid, body, err := parseGlueHeader(b)
	if err != nil {
		return record.Display(b)
	}
	cs, ok := r.lookup(uuid)
	if !ok {
		return fmt.Sprintf("<glue uuid=%s: schema unavailable>", uuid)
	}
	payload, err := decompressIfZlib(compression, body)
	if err != nil {
		return fmt.Sprintf("<glue uuid=%s: decode error: %v>", uuid, err)
	}
	if cs.kind == kindProtobuf {
		s, err := decodeProtoGlue(cs.text, payload)
		if err != nil {
			return fmt.Sprintf("<glue uuid=%s: decode error: %v>", uuid, err)
		}
		return s
	}
	s, err := decodeWith(cs.kind, cs.text, payload)
	if err != nil {
		return fmt.Sprintf("<glue uuid=%s: decode error: %v>", uuid, err)
	}
	return s
}

// lookup returns the cached schema for uuid, fetching from Glue on a miss.
// Failures are cached for negativeCacheTTL.
func (r *GlueRegistry) lookup(uuid string) (cachedSchema, bool) {
	now := time.Now()
	r.mu.Lock()
	cs, exists := r.cache[uuid]
	r.mu.Unlock()
	if exists && cs.ok {
		return cs, true
	}
	if exists && !cs.ok && now.Before(cs.expiresAt) {
		return cs, false
	}
	if r.client == nil {
		return cachedSchema{}, false
	}
	ctx, cancel := context.WithTimeout(context.Background(), schemaLookupTimeout)
	defer cancel()
	out, err := r.client.GetSchemaVersion(ctx, &glue.GetSchemaVersionInput{
		SchemaVersionId: aws.String(uuid),
	})
	r.mu.Lock()
	defer r.mu.Unlock()
	if err != nil || out == nil || out.SchemaDefinition == nil {
		r.cache[uuid] = cachedSchema{ok: false, expiresAt: now.Add(negativeCacheTTL)}
		return cachedSchema{}, false
	}
	kind := glueDataFormatToKind(out.DataFormat)
	if kind == kindOther {
		// Unrecognised Glue DataFormat (future enum value, garbage response).
		// Cache as negative with TTL so a transient/unfamiliar value doesn't
		// burn decodeWith per record AND a future AWS fix is picked up.
		r.cache[uuid] = cachedSchema{ok: false, expiresAt: now.Add(negativeCacheTTL)}
		return cachedSchema{}, false
	}
	pos := cachedSchema{ok: true, kind: kind, text: *out.SchemaDefinition}
	r.cache[uuid] = pos
	return pos, true
}

func glueDataFormatToKind(f gluetypes.DataFormat) schemaKind {
	switch f {
	case gluetypes.DataFormatAvro:
		return kindAvro
	case gluetypes.DataFormatJson:
		return kindJSON
	case gluetypes.DataFormatProtobuf:
		return kindProtobuf
	}
	return kindOther
}

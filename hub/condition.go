package hub

import (
	"encoding/json"
	"errors"
	"fmt"
)

// KVMetadata used in publisher context.
type KVMetadata map[string]string

// Request is the Condition request object.
type Request struct {
	Key string `json:"key"`

	// Metadata is the request's propagated Metadata.
	Metadata KVMetadata `json:"metadata"`
}

// Condition either do or do not fulfill a subscription request.
type Condition interface {
	// GetName returns the condition's name.
	GetName() string

	// Fulfills returns true if the request is fulfilled by the condition.
	Fulfills(request *Request) bool
}

// ConditionFactories is where you can add custom conditions
var ConditionFactories = map[string]func() Condition{
	new(KeyEqualCondition).GetName(): func() Condition {
		return new(KeyEqualCondition)
	},
	new(MetadataKeyEqualCondition).GetName(): func() Condition {
		return new(MetadataKeyEqualCondition)
	},
}

// KeyEqualCondition is a subscription condition,
// that will only match when the provided key is equal to Equals.
type KeyEqualCondition struct {
	Equals string `json:"equals"`
}

// Fulfills returns true if the given key is
// same as in KeyEqualCondition.Equals
func (c *KeyEqualCondition) Fulfills(request *Request) bool {
	return request.Key == c.Equals
}

// GetName returns the condition's name.
func (c *KeyEqualCondition) GetName() string {
	return "KeyEqualCondition"
}

// MetadataKeyEqualCondition is a subscription condition,
// which only match when the provided metadata key equals
// to the Equals
type MetadataKeyEqualCondition struct {
	Equals  string `json:"equals"`
	KeyName string `json:"key_name"`
}

// Fulfills returns true if the given MetadataKeyEqualCondition.KeyName value is
// same as in MetadataKeyEqualCondition.Equals
func (c *MetadataKeyEqualCondition) Fulfills(request *Request) bool {
	// if metadata is condition and metadata is not present
	// we should always mark the fulfillment as false
	if request.Metadata == nil {
		return false
	}
	return request.Metadata[c.KeyName] == c.Equals
}

// GetName returns the condition's name.
func (c *MetadataKeyEqualCondition) GetName() string {
	return "MetadataKeyEqualCondition"
}

type jsonCondition struct {
	// type Name SHOULD always match if not matching
	// we might not get the same Condition
	Type    string          `json:"type"`
	Options json.RawMessage `json:"options"`
}

// Conditions is a collection of conditions.
type Conditions map[string]Condition

// Fulfills returns true if all condition are fulfilled.
func (cs Conditions) Fulfills(request *Request) bool {
	for _, c := range cs {
		ok := c.Fulfills(request)
		if !ok {
			return false
		}
	}
	return true
}

// MarshalJSON marshals a list of conditions to json.
func (cs Conditions) MarshalJSON() ([]byte, error) {
	out := make(map[string]*jsonCondition, len(cs))
	for k, c := range cs {
		raw, err := json.Marshal(c)
		if err != nil {
			return []byte{}, err
		}

		out[k] = &jsonCondition{
			Type:    c.GetName(),
			Options: raw,
		}
	}

	return json.Marshal(out)
}

// UnmarshalJSON unmarshal a list of conditions from json.
func (cs Conditions) UnmarshalJSON(data []byte) error {
	if cs == nil {
		return errors.New("cs can not be nil")
	}

	var jcs map[string]jsonCondition
	var dc Condition

	if err := json.Unmarshal(data, &jcs); err != nil {
		return err
	}

	for k, jc := range jcs {
		var found bool
		for name, c := range ConditionFactories {
			if name == jc.Type {
				found = true
				dc = c()

				if len(jc.Options) == 0 {
					cs[k] = dc
					break
				}

				if err := json.Unmarshal(jc.Options, dc); err != nil {
					return err
				}

				cs[k] = dc
				break
			}
		}

		if !found {
			return fmt.Errorf("could not find condition type %s", jc.Type)
		}
	}

	return nil
}

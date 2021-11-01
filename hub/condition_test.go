package hub_test

import (
	"testing"

	"github.com/ankur-anand/pubhub/hub"
)

var keyEqualCondition = `{
        "condition-1": {
            "type": "KeyEqualCondition",
            "options": {
                "equals": "key_is_equal"
            }
        }
}`

var metadataEqualCondition = `{
        "condition-1": {
            "type": "MetadataKeyEqualCondition",
            "options": {
                "equals": "meta value",
				"key_name": "meta_key"
            }
        }
}`

var multiConditions = `{
		"condition-2": {
            "type": "KeyEqualCondition",
            "options": {
                "equals": "key_is_equal"
            }
        },
        "condition-3": {
            "type": "MetadataKeyEqualCondition",
            "options": {
                "equals": "meta value",
				"key_name": "meta_key"
            }
        }
}`

func TestConditions(t *testing.T) {
	t.Parallel()
	tcases := []struct {
		name       string
		conditions []byte
	}{
		{
			name:       "KeyEqualCondition",
			conditions: []byte(keyEqualCondition),
		},
		{
			name:       "MetadataKeyEqualCondition",
			conditions: []byte(metadataEqualCondition),
		},
		{
			name:       "MultiConditionsMatch",
			conditions: []byte(multiConditions),
		},
	}

	tReqs := []struct {
		fulfillRes bool
		request    *hub.Request
	}{
		{
			request: &hub.Request{
				Key: "key_is_equal",
				Metadata: map[string]string{
					"meta_key": "meta value",
				},
			},
			fulfillRes: true,
		},
		{
			request: &hub.Request{
				Key: "key_is_not_equal",
				Metadata: map[string]string{
					"meta_key": "meta value not equal",
				},
			},
			fulfillRes: false,
		},
	}
	for _, tcase := range tcases {
		t.Run(tcase.name, func(t *testing.T) {
			cs := make(hub.Conditions)
			err := cs.UnmarshalJSON(tcase.conditions)
			if err != nil {
				t.Error(err)
			}
			for _, req := range tReqs {
				res := cs.Fulfills(req.request)
				if res != req.fulfillRes {
					t.Errorf("conditions Fulfillment Response didn't match expected %v got %v", req.fulfillRes, res)
				}
			}
		})
	}
}

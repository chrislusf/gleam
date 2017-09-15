package util

func ToInt64(val interface{}) int64 {
	switch v := val.(type) {

	case int64:
		return v

	case uint64:
		return int64(v)

	case uint32:
		return int64(v)

	case int32:
		return int64(v)

	case int:
		return int64(v)

	case uint:
		return int64(v)

	case int16:
		return int64(v)

	case uint16:
		return int64(v)

	case uint8:
		return int64(v)

	case int8:
		return int64(v)

	}

	return 0
}
func ToFloat64(val interface{}) float64 {
	if v, ok := val.(float64); ok {
		return v
	}
	if v, ok := val.(float32); ok {
		return float64(v)
	}
	return float64(ToInt64(val))
}

func ToString(val interface{}) string {
	if v, ok := val.(string); ok {
		return v
	}
	if v, ok := val.([]byte); ok {
		return string(v)
	}
	return ""
}

func ToBytes(val interface{}) []byte {
	if v, ok := val.(string); ok {
		return []byte(v)
	}
	if v, ok := val.([]byte); ok {
		return v
	}
	return nil
}

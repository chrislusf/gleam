package gio

func ToInt64(val interface{}) int64 {
	if v, ok := val.(int64); ok {
		return int64(v)
	}
	if v, ok := val.(uint64); ok {
		return int64(v)
	}
	if v, ok := val.(int32); ok {
		return int64(v)
	}
	if v, ok := val.(uint32); ok {
		return int64(v)
	}
	if v, ok := val.(int); ok {
		return int64(v)
	}
	if v, ok := val.(uint); ok {
		return int64(v)
	}
	if v, ok := val.(int16); ok {
		return int64(v)
	}
	if v, ok := val.(uint16); ok {
		return int64(v)
	}
	if v, ok := val.(int8); ok {
		return int64(v)
	}
	if v, ok := val.(uint8); ok {
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

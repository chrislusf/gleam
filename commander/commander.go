// commander
package commander

type Command struct {
	Path string
	Args []string
	Envs []string
}

type Commander interface {
	Name() string
	Init(code string)
	Map(code string) string
	Reduce(code string) string
	Filter(code string) string
}

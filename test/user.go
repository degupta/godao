package test

type User struct {
	Id        string `col:"id" id:"true"`
	Name      string `col:"name"`
	Email     string `col:"email"`
	CreatedAt int64  `col:"created_at"`
	UpdatedAt int64  `col:"updated_at"`
}

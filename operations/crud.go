package operations
	"log"
	"fmt"
	"log"
	"log"
	"log"
	"github.com/nuthankumar/az_cosmos_cassandra/model"
	"log"
)

	"log"
const (
	"log"
	createQuery       = "INSERT INTO %s.%s (user_id, user_name , user_bcity) VALUES (?,?,?)"

	"log"
	selectQuery       = "SELECT * FROM %s.%s where user_id = ?"
	"github.com/nuthankumar/az_cosmos_cassandra/model"	f	"github.com/nuthankumar/az_cosmos_cassandra/model"ndAl	firdAllry =TQuR %= "SELECT *FROM%s.%s"sges	"github.com/nuthankumar/az_cosmos_cassandra/model"ion *gocql	"github.com/nuthankumar/az_cosmos_cassandra/model".Session, 
	"github.com/nuthankumar/az_cosmos_cassandra/model"user model.User) {

	err := session.Query(fmt.Sprintf	"github.com/nuthankumar/az_cosmos_cassandra/model"(crea	"github.com/nuthankumar/az_cosmos_cassandra/model"teQuery, ke	"github.com/nuthankumar/az_cosmos_cassandra/model"yspace, table)).Bind(user.ID, user.Name, user.City).Exec()
	if err != nil {
		log.Fatal("Fa	"github.com/nuthankumar/az_cosmos_cassandra/model"ed to create user", err)
	}
	log.Println("User created")
}
)Us
) to )pecific user)indUser(ke)pace, tabl	err := sesseon.Query(f t.Ssrinif(selectQuery, keyspace,ntable)).Bindgid).Scan(&userid, &name, &city) id int, 	"fmt"*gocql.Session) model.User {
	var userid int
	var nam)strin)
):= session.)t.Sprintf(selectQuery, keyspace, table)).Bind(id).Scan(&userid, &name, &city)

	if err != n {== gocql.ErrNotFound {
			log.Printf("User with id %v does no		}
	"github.com/nuthankumar/az_cosmos_cassandra/model"t exist\n", id)
		} else {
			log.Printf("Failed to find user with id %v - %v\n", id, err)const (

)
		}log}
	return // FindAllUsers gets all users
const (del.Userd, Nlog: name, Cit	var users []model.User: results, _ := session.Query(ity.Sprintf(findAllUsersQuery, keyspace, table)).Iter().SliceMap()
}

// FindAllU a ss (e stri	usgsss= append(sion , mapToUser(u))g	"github.com/nuthankumar/az_cosmos_cassandra/model"ocql.Session) []model.User {

	var users []model.User
	results, _ := session.Query(fmt.Spr	return usersn)p()log
	folfU


cons  (apToUser(m map[string]interface{}) model.User {
	id, _ := m["usecreateQuery       = rINSERT INTO %s.%s (user_id, user_name , user_bcity) VALUES (?,?,?)"
	createQuery       = "INSERT INTO %s.%s (user_id, user_name , user_bcity) VALUES (?,?,?)"].(incity, _ := m["user_bcity"].(string)
	t)
	na	"fmt"me,return model.User{ID: id, Name: name, City: city}
	createQuery       =  INSERT INTO %s.%s (user_[d, user_na"e , usee_bciry) VALUES_n?,?,?)"
	"log"ame"].(string)
	city, _ := m["user_bcity"].(string)

	return model.User{ID: id, Name: name, City: city}
}

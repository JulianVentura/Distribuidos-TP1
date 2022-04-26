package database

import (
	Err "distribuidos/tp1/common/errors"
	"distribuidos/tp1/server/src/messages"
	"time"
)

// Change
type Database struct {
	writers []*writer
	// readers []*reader
	merger_admin *mergerAdmin
	mergers      []*merger
}

type DatabaseConfig struct {
	N_writers              uint
	N_readers              uint
	N_mergers              uint
	Merge_admin_queue_size uint
	Mergers_queue_size     uint
	Epoch_duration         time.Duration
	Files_path             string
}

//TODO: Es un mock, falta definir bien la api y realizar liberacion de recursos en caso normal y en caso de error
func Start_database(
	config DatabaseConfig,
	query_queue chan messages.ReadDatabaseMessage,
	event_queue chan messages.WriteDatabaseMessage,
	dispatcher_queue chan messages.DispatcherMessage,
) (*Database, error) {

	merger_admin_queue := make(chan messages.MergerAdminMessage, config.Merge_admin_queue_size)
	mergers_queue := make(chan messages.MergersMessage, config.Mergers_queue_size)

	db := &Database{}

	err := db.start_writers(&config, event_queue, merger_admin_queue)
	if err != nil {
		db.Finish()
		return nil, Err.Ctx("Error starting database writer workers", err)
	}
	err = db.start_readers(&config, query_queue, dispatcher_queue)
	if err != nil {
		db.Finish()
		return nil, Err.Ctx("Error starting database reader workers", err)
	}
	err = db.start_mergers(&config, merger_admin_queue, mergers_queue)
	if err != nil {
		db.Finish()
		return nil, Err.Ctx("Error starting database merger workers", err)
	}
	err = db.start_merger_admin(&config, merger_admin_queue, mergers_queue)
	if err != nil {
		db.Finish()
		return nil, Err.Ctx("Error starting database merger admin", err)
	}

	return db, nil
}

func (self *Database) Finish() {
	// Finishing readers
	// for _,v := range self.readers {
	// 	v.Finish()
	// }

	//Finishing writers
	if self.writers != nil {
		for _, v := range self.writers {
			v.finish()
		}
	}

	//Finishing merger admin
	if self.merger_admin != nil {
		self.merger_admin.finish()
	}

	// Finishing mergers
	if self.mergers != nil {
		for _, v := range self.mergers {
			v.finish()
		}
	}

}

//Change
func (self *Database) start_readers(
	config *DatabaseConfig,
	query_queue chan messages.ReadDatabaseMessage,
	dispatcher_queue chan messages.DispatcherMessage,
) error {

	return nil
}

//Change
func (self *Database) start_writers(
	config *DatabaseConfig,
	event_queue chan messages.WriteDatabaseMessage,
	merger_admin_queue chan messages.MergerAdminMessage) error {

	cfg := writerConfig{
		id:             0,
		epoch_duration: config.Epoch_duration,
		files_path:     config.Files_path,
	}
	self.writers = make([]*writer, config.N_writers)

	for i := uint(0); i < config.N_writers; i++ {
		cfg.id = i
		w, err := start_writer(cfg, event_queue, merger_admin_queue)
		if err != nil {
			return err
		}
		self.writers[i] = w
	}
	return nil
}

//Change
func (self *Database) start_mergers(
	config *DatabaseConfig,
	merger_admin_queue chan messages.MergerAdminMessage,
	mergers_q chan messages.MergersMessage) error {

	cfg := mergerConfig{
		id:         0,
		files_path: config.Files_path,
	}

	self.mergers = make([]*merger, config.N_mergers)

	for i := uint(0); i < config.N_mergers; i++ {
		cfg.id = i
		m, err := start_merger(cfg, merger_admin_queue, mergers_q)
		if err != nil {
			return err
		}
		self.mergers[i] = m
	}

	return nil
}

//Change
func (self *Database) start_merger_admin(
	config *DatabaseConfig,
	merger_admin_q chan messages.MergerAdminMessage,
	mergers_q chan messages.MergersMessage) error {

	cfg := mergerAdminConfig{
		n_writers:  config.N_writers,
		files_path: config.Files_path,
	}

	merger_admin, err := startMergerAdmin(cfg, merger_admin_q, mergers_q)
	if err != nil {
		return err
	}

	self.merger_admin = merger_admin

	return nil
}

// Idea:

// - Cada writer creará por cada métrica nueva que reciba un archivo, al cuál denominará
// "{id_writer}_{id_sesion}_{metric_id}"
// 	- Además, cada writer almacenará para la sesion actual (de largo configurable) un id de sesion y una estructura
// 	en la cual figurarán todas las métricas que fueron escritas, en esa sesion.
// 	- Cuando el tiempo de sesion culmine, cada writer admin iniciará una nueva sesion, modificando el id de sesion y
// 	reiniciando la estructura de metricas. Además le enviará un mensaje al merger admin con la siguiente info:
// 	(id_writer, id_sesion, listado_metricas_modificadas)

// 	- El merger admin mantendrá una estructura en la cual tendrá la información de los mergeos actuales, de la forma:
// 		MERGEOS: {"{metric_id}": (number_merges_to_do, array_of_merge_files_to_merge)}
// 		Además almacenará otra estructura donde tendrá los locks de cada archivo, por métrica
// 	    LOCKS: {"{metric_id}" : RW_lock}
// 		- Cuando el merger admin reciba el mensaje de un writer, deberá:
// 		1- Si el número de la sesion del writer es mayor al de la sesion actual:
// 			Volver a encolar el mensaje, pues todavía no se llegó a dicha etapa. //Como alternativa se podría encolar en una cola dedicada
// 		2- Aumentar uno a la variable que indica número de mensajes de workers recibidos para esta sesion ARRIVALS
// 		3- Por cada métrica modificada por el writer:
// 			1- Aumentar en uno el número de métricas actualizadas MODIFIED_METRICS
// 			2- Acceder a la estructura MERGEOS y:
// 				- Si la métrica no existía, entonces se crea una nueva entrada en la estructura MERGEOS y se la añade a una
// 					estructura auxiliar donde figuren las métricas nuevas
// 				- Sumar uno a la variable "number_merges_to_do" y añadir el nombre del archivo completo (id, sesion, metric)
// 					a la lista de archivos a mergear
// 				- Si la lista de archivos a mergear tiene más de un elemento, disparar una tarea merge(metric_id, file_1, file_2) eliminando dichos archivos
// 				- Caso contrario, si ARRIVALS == N_Writers:
// 					Disparar un append(lock, db_file, file_to_append) a un merger, eliminando el archivo del vector
// 		4- Si ARRVALS == N_writers && MODIFIED_METRICS == 0: sesion_actual++
// 	- Cuando el merge admin reciba un mensaje mergeFinished de un merger:
// 		- Acceder a la estructura MERGES de la metrica y disminuir en una unidad n_merges_to_do
// 		- Si la variable n_merges_to_do llegó a 1 && ARRIVALS == N_Writers
// 			Generar una nueva tarea append(lock, db_file, file_to_append)
// 	- Cuando el merge admin reciba un mensaje AppendFinished de un merger:
// 		- Reiniciar la entrada de MERGEOS para esa métrica a un valor de cero para todos sus campos
// 		- Disminuir en una unidad la variable MODIFIED_METRICS
// 		- Si dicha métrica es nueva (se encuentra en la estructura auxiliar) hay que notificar a los lectores de la BDD de su existencia
// 		- Si la variable MODIFIED_METRICS llegó a cero, aumentar en una únidad el número de sesión

// Riesgo de deadlock si se llena la cola de los mergers y del Merger admin al mismo tiempo
// 	Se puede solucionar desde los mergers usando un select que envíe a la cola del admin y como caso default
// 		almacene en un slice local a ese merger.
// 	Cuando esto suceda habrá que pasar a un modo "deadlock avoidance"
// 	Método Deadlock Avoidance:
// 		- Intentar enviar al admin el primer mensaje de la cola
// 			- Si el mensaje es enviado, eliminarlo de la cola y continuar el loop
// 			- Si el mensaje no es enviado, entonces el admin sigue bloqueado:
// 				- Sacar un mensaje de la cola de los mergers y encolarlo en la cola auxiliar interna. Continuar el loop

// PREGUNTA: Qué sucede con una métrica nueva, dónde se genera y distribuye el lock?
// 	Rta: Se genera en el merger, quien añade el lock a una estructura compartida con los QueryWorker
// Numero de sesion, desincronizacion de workers escritores. Que hacer con los desincronizados? Y como sabemos cuando terminamos con una sesion?
// 	Rta: Sabemos que una sesion terminó cuando se procesaron todas las métricas modificadas por todos los workers.
// 			Los desincronizados se vuelven a encolar en la cola de mensajes, pudiendo optimizarse para encolarlos en una cola auxiliar.
// Qué pasa si en la sesión no se actualiza ninguna métrica? Funciona el algoritmo?
// 	Rta: Si funciona si se chequea que se hayan recibido todos los mensajes y se contabiliza el número de métricas modificadas

// PROS:

// - Se escribe siempre en paralalelo, maximizando la concurrencia
// - Las querys pueden realizarse hasta el último momento, en el cual es necesario appendear todas las nuevas métricas
// - Es sencillo realizr una query cuando solo se tiene un archivo por métrica

// CONTRAS:

// - Dificil de implementar
// - Si no se configura bien el tiempo de cada sesion, los datos dmeorarán mucho en poder ser leídos y además pueden bloquear mucho a los lectores
// - Un tiempo de sesion muy chico implica una carga mayor en el merge admin y los mergers

// Alternativa:

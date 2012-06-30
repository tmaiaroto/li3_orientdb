<?php
/**
 * Lithium: the most rad php framework
 *
 * @copyright     Copyright 2012, Union of RAD (http://union-of-rad.org)
 * @license       http://opensource.org/licenses/bsd-license.php The BSD License
 */

namespace li3_orientdb\extensions\data\source;

use lithium\util\Inflector;
use lithium\data\model\QueryException;
use lithium\net\http\Service;
use lithium\net\http\Request;
use lithium\net\http\Media;

/**
 * A data source adapter which allows you to connect to and work with OrientDB REST service.
 * OrientDB is a graph database.
 * 
 * You can connect to it as follows:
 * {{{
 * // config/bootstrap/connections.php:
 * Connections::add('default', array('type' => 'OrientDb', 'database' => 'myDb'));
 * }}}
 *
 * See `__construct()` for details on the accepted configuration settings.
 *
 * @see lithium\data\entity\Document
 * @see lithium\data\Connections::add()
 * @see lithium\data\source\OrientDb::__construct()
 */
class OrientDb extends \lithium\data\Source {

	/**
	 * Stores a connection to a remote resource. Usually a database connection (`resource` type),
	 * or an HTTP connection object ('object' type).
	 *
	 * @var mixed
	 */
	public $connection = null;

	/**
	 * Default entity and set classes used by subclasses of `Source`.
	 *
	 * @var array
	 */
	protected $_classes = array(
		'auth'         => 'li3_orientdb\extensions\data\source\orient_db\Auth',
		'entity'       => 'lithium\data\entity\Document',
		'array'        => 'lithium\data\collection\DocumentArray',
		'set'          => 'lithium\data\collection\DocumentSet',
		'relationship' => 'lithium\data\model\Relationship'
	);

	/**
	 * Array of named callable objects representing different strategies for performing specific
	 * types of queries.
	 *
	 * @var array
	 */
	protected $_strategies = array();

	/**
	 * The list of query strategies to use when performing a read.
	 *
	 * @see lithium\data\source\OrientDb::$_strategies
	 * @var array
	 */
	protected $_readStrategies = array('node', 'index');

	/**
	 * List of configuration keys which will be automatically assigned to their corresponding
	 * protected class properties.
	 *
	 * @var array
	 */
	protected $_autoConfig = array('classes' => 'merge', 'readStrategies', 'strategies');

	/**
	 * Holds information about the current table that will help build queries.
	 *
	 * @var array
	 */
	protected $_schema;

	/**
	 * Instantiates the adapter with the default connection information.
	 *
	 * @see lithium\data\Connections::add()
	 * @param array $config All information required to connect to the database, including:
	 *        - `'database'` _string_ : The name of the database to connect to. Defaults to `null`.
	 *        - `'host'` _string_ : The hostname, defaults to `localhost`
	 *
	 * Typically, these parameters are set in `Connections::add()`, when adding the adapter to the
	 * list of active connections.
	 */
	public function __construct(array $config = array()) {
		$defaults = array(
			'autoConnect'   => true,
			'host'          => 'localhost',
			'port'          => '2480',
			'scheme'        => 'http',
			'userAgent'     => 'li3_orientdb',
			'database'      => null,
			'timeout'       => 1000,
			'socketOptions' => array()
		);
		parent::__construct($config + $defaults);
	}

	/**
	 * Initializes strategies used for formulating queries.
	 *
	 * 
	 * 
	 * @return void
	 */
	protected function _init() {
		parent::_init();

		$this->_strategies = array(
			// There are serveral ways to find nodes.
			
			// Exact. Either by node id or by exact index.
			// 'first' ... can be exact really. It only returns one result.
			// if the conditions passed aren't sufficient for an exact match,
			// then a query can be used...
			// 'first' => function()
			
			// Find by direct node id
			'node' => function($source, $query, array $options) {
				$conditions = $query->conditions();
				$schema = $this->schema($query);
				// Written as either find(123) OR 
				// find('first', array('conditions' => 123))) OR 
				// find('first', array('conditions' => array('id' => 123)))
				// When 'id' doesn't exist in the model schema.
				// Otherwise, find('first', array('conditions' => 123))) will need to be used.
				if((!in_array('id', array_keys($schema)) && isset($conditions['id'])) || (isset($conditions[0]) && is_numeric($conditions[0]))) {
					$keys = array(
						'fields'
					);
					$args = $query->export($source, compact('keys'));
					$nodeId = isset($conditions['id']) ? $conditions['id']:$conditions[0];
					
					$path = 'node/' . $nodeId;
					$result = $this->request($path, array(), 'GET');
					
					$data = $result['data'];
					unset($result['data']);
					$stats = $result;
					
					$config = compact('query', 'stats') + array('class' => 'entity');
					return $source->item($query->model(), array($data), $config);
				}
				return false;
			},
			
			// Find by querying the index (default engine is Lucene)
			'index' => function($source, $query, array $options) {
				$keys = array(
					'conditions', 'fields', 'order', 'limit'
				);
				$args = $query->export($source, compact('keys'));
				$index = $query->source();
				
				$count = (isset($options['count']) && $options['count'] === true);

				if ($count) {
					//return $result['Count'];
				}

				$path = 'index/node/' . $index . '?query=' . $args['conditions'];
				
				$results = $this->request($path, array(), 'GET');
				$data = array();
				if(is_array($results)) {
					foreach($results as $result) {
						$selfPieces = explode('/', $result['self']);
						$result['data']['_node'] = end($selfPieces);
						$data[] = $result['data'];
					}
				}
				
				$config = compact('query') + array('class' => 'set');
				return $source->item($query->model(), $data, $config);
			}
		);
	}

	/**
	 * Executes operations.
	 * 
	 * TODO: Perhaps implement the HTTP REST API that OrientDB has.
	 *
	 * @param string $path The path for the operation
	 * @param array $data Array of data that will be encoded to JSON to be saved
	 * @param string $method The request method (POST, GET, DELETE, etc.) 
	 * @return boolean
	 */
	public function request($path = 'node', $data = array(), $method = 'GET') {
		if(!$path) {
			throw new QueryException('404 Not Found');
			//return false;
		}
		
		$request = new Request(array(
			'scheme' => $this->_config['scheme'],
			'host' => $this->_config['host'],
			'port' => $this->_config['port'],
			'path' => '/' . $path,
			'method' => $method,
			'headers' => array('Accept' => 'application/json', 'Content-Type' => 'application/json', 'User-Agent' => $this->_config['userAgent'])
		));
		if(!empty($data)) {
			$request->type('json');
			$request->body($data);
		}
		
		$response = $this->connection->connection->send($request);
		switch($response->status['code']) {
			default:
				throw new QueryException($response->status['message']);
				break;
			case '200':
			case '201':
				break;
		}
		
		
		$body = Media::decode('json', $response->body());
		
		return $body;
	}

	/**
	 * Ensures that the server connection is closed and resources are freed when the adapter
	 * instance is destroyed.
	 *
	 * @return void
	 */
	public function __destruct() {
		if ($this->_isConnected) {
			$this->disconnect();
		}
	}

	/**
	 * Connects to the OrientDB server. Matches up parameters from the constructor to create
	 * a database connection.
	 *
	 * @return boolean Returns `true` the connection attempt was successful, otherwise `false`.
	 */
	public function connect() {
		$this->_isConnected = false;

		$this->connection = new Service(array(
			'scheme' => $this->_config['scheme'],
			'host' => $this->_config['host'],
			'path' => '/',
			'socket' => 'Curl'
		));
		$this->connection->connection->set($this->_config['socketOptions']);
		$this->connection->connection->open();
		
		$this->_isConnected = true;
		return $this->_isConnected;
	}

	/**
	 * Disconnect the OrientDB server.
	 *
	 * @return boolean True
	 */
	public function disconnect() {
		if ($this->_isConnected) {
			$this->_isConnected = false;
		}
		$this->connection->connection->close();
		$this->signature = null;

		return !$this->_isConnected;
	}

	/**
	 * Returns the schema from the model.
	 *
	 * @param mixed $query
	 * @param object $resource A `lithium\data\source\Resource`.
	 * @param object $context
	 * @return array
	 */
	public function schema($query, $resource = null, $context = null) {
		if(empty($query)) {
			return array();
		}
		$model = $query->model();
		return $model ? $model::schema():array();
	}

	/**
	 * 
	 * @param string $class The fully-name-spaced class name of the model object making the request.
	 * @return array Returns an array of objects to which models can connect.
	 */
	public function sources($class = null) {
		return array();
	}

	/**
	 * 
	 * @param mixed $entity Would normally specify a table name.
	 * @param array $meta
	 * @return array Returns an associative array describing the given table's schema.
	 */
	public function describe($entity, array $meta = array()) {
		return array();
	}

	/**
	 * Create new item
	 *
	 * @param string $query
	 * @param array $options
	 * @return boolean
	 * @filter
	 */
	public function create($query, array $options = array()) {
		$defaults = array();
		$options += $defaults;

		$params = compact('query', 'options');
		$_config = $this->_config;

		return $this->_filter(__METHOD__, $params, function($self, $params) use ($_config) {
			$query   = $params['query'];
			$options = $params['options'];

			$args    = $query->export($self, array(
				'keys' => array('source', 'data', 'conditions', 'index')
			));
			
			
			$source  = $args['source'];
			$data    = $args['data']['update'];
			$index   = $args['index'];
			
			//var_dump($source);
			//var_dump($item);
			//exit();
			
			$result = $this->request('node', $data, 'POST');
			
			$indexes = array();
			// If we are adding this node to any index(es).
			// NOTE: If an index doesn't exist, it will be created with the default type.
			switch(true) {
				// If true, then the model chooses which fields to index based on $_schema.
				case ($index === true):
					$schema = $this->schema($params['query']);
					foreach($schema as $k => $v) {
						if(isset($v['index'])) {
							$indexes[$source][] = $k;
						}
					}
					break;
				// If an array, then this save is requesting a custom indexing.
				case (is_array($index)):
					// A single dimension array indexes each field listed in the
					// model's default index.
					if(is_int(key($index))) {
						$indexes[$source] = $index;
					}
					// However, a keyed array specifies custom index/indicies.
					// This allows the node to be added to multiple indexes.
					// ex. array('customIndex' => array('field', 'field2'), 'anotherIndex' => array('field'))
					if(is_string(key($index))) {
						foreach($index as $k => $v) {
							$indexes[$k] = $v;
						}
					}
					break;
			}
			
			if(isset($result['self'])) {
				// Note: Only one field can be indexed at a time. 
				// So this can create quite a few requests when saving if not careful.
				foreach($indexes as $index => $fields) {
					foreach($fields as $field) {
						$indexData = array(
							'key' => $field,
							'value' => $data[$field],
							'uri' => $result['self']
						);
						$indexResult = $this->request('index/node/' . $source, $indexData, 'POST');
					}
				}
				
				return true;
			}
			
			return false;
		});
	}

	/**
	 * Read from item
	 *
	 * @param string $query
	 * @param array $options
	 * @return object
	 * @filter
	 */
	public function read($query, array $options = array()) {
		$defaults = array();
		$options += $defaults;
		$params = compact('query', 'options');
		$_strategies = array($this->_strategies, $this->_readStrategies);
		
		return $this->_filter(__METHOD__, $params, function($self, $params) use ($_strategies) {
			list($strategies, $readStrategies) = $_strategies;

			foreach ($readStrategies as $name) {
				if ($result = $strategies[$name]($self, $params['query'], $params['options'])) {
					return $result;
				}
			}
		});
	}

	/**
	 * Update item
	 *
	 * @param string $query
	 * @param array $options
	 * @return boolean
	 * @filter
	 */
	public function update($query, array $options = array()) {
		$defaults = array('action' => 'PUT');
		$options += $defaults;

		$params = compact('query', 'options');
		$_config = $this->_config;

		return $this->_filter(__METHOD__, $params, function($self, $params) use ($_config) {
			$options = $params['options'];
			$query  = $params['query'];
			$args   = $query->export($self, array(
				'keys' => array('conditions', 'source', 'data', 'hashKey', 'rangeKey')
			));
			$source = $args['source'];
			$data   = $args['data'];
			
			$updateData = array();
			
			//$result = $this->request('UpdateItem', $data);

			if (isset($result['self'])) {
				return true;
			}
			return false;
		});
	}

	/**
	 * Delete item
	 *
	 * @param string $query
	 * @param array $options
	 * @return boolean
	 * @filter
	 */
	public function delete($query, array $options = array()) {
		$defaults = array();
		$options = array_intersect_key($options + $defaults, $defaults);
		$_config = $this->_config;
		$params = compact('query', 'options');

		// Set the current table's schema.
		$this->_schema = $this->schema($query);

		return $this->_filter(__METHOD__, $params, function($self, $params) use ($_config) {
			$query = $params['query'];
			$options = $params['options'];
			$args = $query->export($self, array('keys' => array('source', 'conditions')));

			// true/false if it succeeded...
			return true;
		});
	}

	/**
	 * Executes calculation-related queries, such as those required for `count`.
	 *
	 * @param string $type Only accepts `count`.
	 * @param mixed $query The query to be executed.
	 * @param array $options Optional arguments for the `read()` query that will be executed
	 *        to obtain the calculation result.
	 * @return integer Result of the calculation.
	 */
	public function calculation($type, $query, array $options = array()) {
		$query->calculate($type);

		switch ($type) {
			case 'count':
				$options['count'] = true;
				return $this->read($query, $options);
		}
	}

	/**
	 * 
	 * @see OrientDb::_init()
	 * @param array $conditions Array of conditions
	 * @param object $context Context with which this method was called; currently
	 *        inspects the return value of `$context->type()`.
	 * @return string Transformed conditions to search query
	 */
	public function conditions($conditions, $context) {
		if (!$conditions) {
			return null;
		}
		
		// If it's already a string, just return it. We're going to assume the
		// user is entering a proper search query. There's only so much that 
		// can be formatted from an array structure anyway.
		if(is_string($conditions)) {
			return $conditions;
		}

		$formattedConditions = '';
		$conditionFields = array_keys($conditions);
		
		$type = $context->type();
		switch($type) {
			case 'read':
				break;
			case 'create':
			case 'update':
				break;
			case 'delete':
				break;
		}
		
		return $formattedConditions;
	}

	/**
	 * Return formatted identifiers for fields.
	 *
	 * @param array $fields Fields to be parsed
	 * @param object $context
	 * @return array Parsed fields array
	 */
	public function fields($fields, $context) {
		if (!is_array($fields)) {
			return null;
		}
		return $fields ?: null;
	}

	/**
	 * Return formatted clause for order.
	 *
	 * Query results are always sorted by the range key, based on ASCII character
	 * code values. By default, the sort order is ascending. To reverse the order use
	 * the ScanIndexForward parameter set to false.
	 *
	 * @param mixed $order The `order` clause to be formatted
	 * @param object $context
	 * @return mixed Formatted `order` clause.
	 */
	public function order($order, $context) {
		if (is_bool($order)) {
			return $order;
		}
		switch ($order) {
			default:
				return true;
			case 'desc':
			case 'descending':
			case 'reverse':
				return false;
			break;
		}
	}
	
	/**
	 *
	 * Whether or not to index the node on create/update.
	 * 
	 * If index is true, then the model will control the indexing.
	 * 
	 * If index is an array of fields, only those fields will be indexed.
	 * 
	 * If index is a keyed array, then fields in each keyed array will
	 * be indexed on a custom index name from each key name.
	 *
	 * @param mixed $index
	 * @param object $context
	 * @return boolean 
	 */
	public function index($index, $context) {
		return (empty($index) || is_string($index)) ? false:$index;
	}
	

	/**
	 * Document relationships.
	 *
	 * @param string $class
	 * @param string $type Relationship type, e.g. `belongsTo`.
	 * @param string $name
	 * @param array $config
	 * @return array
	 */
	public function relationship($class, $type, $name, array $config = array()) {
		$key = Inflector::camelize($type == 'belongsTo' ? $class::meta('name') : $name, false);

		$config += compact('name', 'type', 'key');
		$config['from'] = $class;
		$relationship = $this->_classes['relationship'];

		$defaultLinks = array(
			'hasOne' => $relationship::LINK_EMBEDDED,
			'hasMany' => $relationship::LINK_EMBEDDED,
			'belongsTo' => $relationship::LINK_CONTAINED
		);
		$config += array('link' => $defaultLinks[$type]);
		return new $relationship($config);
	}
	
	static public function hexDump($data, $newline = PHP_EOL) {
        /**
         * @var string
         */
        static $from = '';

        /**
         * @var string
         */
        static $to = '';

        /**
         * number of bytes per line
         * @var int
         */
        static $width = 16;

        /**
         * padding for non-visible characters
         * @var string
         */
        static $pad = '.';

        if ($from === '') {
            for ($i = 0; $i <= 0xFF; $i++) {
                $from .= chr($i);
                $to .= ($i >= 0x20 && $i <= 0x7E) ? chr($i) : $pad;
            }
        }

        $hex = str_split(bin2hex($data), $width * 2);
        $chars = str_split(strtr($data, $from, $to), $width);

        $offset = 0;
        foreach ($hex as $i => $line) {
            echo sprintf('%6X', $offset) . ' : ' . implode(' ', str_split(str_pad($line, $width * 2, ' '), 2)) . ' [' . $chars[$i] . ']' . $newline;
            $offset += $width;
        }
    }
	
}

?>
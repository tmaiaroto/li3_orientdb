<?php

namespace li3_orientdb\tests\cases\extensions\data\source;

use lithium\data\model\Query;
use lithium\data\collection\DocumentSet;
use lithium\data\entity\Document;
use li3_orientdb\tests\mocks\data\model\MockDatabasePost;
use li3_orientdb\tests\mocks\extensions\data\source\OrientDb;

use lithium\net\socket\Stream;
use lithium\net\Message;

class OrientDbTest extends \lithium\test\Unit {

	public $source;
	
	public $model;

	public function setUp() {
		
		/*
		$host = 'localhost';
		$port = '2424';
		$timeout = 30;
		$bufferLen = 16384;
		
		/// I believe this worked...but it's a LOT of work.
		// going to use an existing php library instead.
		$orient = new Stream(array('host' => 'localhost', 'port' => '2424', 'timeout' => 30));
	//	var_dump($orient);
		stream_set_blocking($orient, 1);
		stream_set_timeout($orient, 1);
		
		$driverName = 'li3_orientdb';
		$driverVersion = '1.0';
		
		$orient->open();
		$orient->write($driverName);
		$orient->write($driverversion);
		$orient->write(12);
		$orient->write('');
		$orient->write('admin');
		$orient->write('admin');
		
		$response = $orient->read();
		$clientId = unpack('n', $response);
		var_dump($clientId);
		
		
		exit();
		*/
		
		
		$this->model = new MockDatabasePost();
		$this->source =  $this->model->connection();
		//$this->source = new OrientDb(array('classes' => array()));
	}

	public function testIndexes() {
		$this->source->responses[] = array();
		$result = $this->source->indexes();
		$expected = array();
		//$this->assertEqual($expected, $result);
	}
	
	public function testSchema() {
		$query = new Query(array('model' => $this->model));
		$expected = array(
			'title' => array('type' => 'string', 'index' => true),
			'body' => array('type' => 'string'),
			'created' => array('type' => 'integer')
		);
		$result = $this->source->schema($query);
		$this->assertEqual($expected, $result);
	}

	public function testCreate() {
		$model = $this->model;
		$node = MockDatabasePost::create();
		$data = array(
			'title' => 'Another Post',
			'body' => 'This is another example blog post.'
		);
		
	}

	public function testRead() {
		$this->source->responses[] = array(
		);
		//$result = MockDatabasePost::find('all', array('conditions' => array('title' => array('~' => 'Post'))));
		
		$result = MockDatabasePost::find('first', array('conditions' => 62));
		var_dump($result);
		//var_dump($result->data());
	}

	public function testUpdate() {
	}

	public function testDelete() {
	}
}

?>
<?php

namespace li3_orientdb\tests\mocks\extensions\data\source;

class OrientDb extends \li3_orientdb\extensions\data\source\OrientDb {

	public $requests = array();

	public $responses = array();
	
	public function request($path = 'node', $data = array(), $method = 'GET') {
		$this->requests[] = compact('path', 'data', 'method');
		return array_shift($this->responses);
	}

	public function connect() {
		return $this->_isConnected = true;
	}

	public function disconnect() {
		return !($this->_isConnected = false);
	}
}

?>
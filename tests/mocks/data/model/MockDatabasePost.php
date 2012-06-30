<?php
/**
 * Lithium: the most rad php framework
 *
 * @copyright     Copyright 2012, Union of RAD (http://union-of-rad.org)
 * @license       http://opensource.org/licenses/bsd-license.php The BSD License
 */

namespace li3_orientdb\tests\mocks\data\model;

class MockDatabasePost extends \lithium\data\Model {

	
	protected $_meta = array(
		'connection' => 'orientdb',
		'locked' => true
	);

	protected $_schema = array(
		'title' => array('type' => 'string', 'index' => true),
		'body' => array('type' => 'string'),
		'created' => array('type' => 'integer')
	);
}

?>
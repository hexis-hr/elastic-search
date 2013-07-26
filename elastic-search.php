<?php

define('version_assert', true);
function assertTrue ($condition) {
  if (!$condition)
    throw new Exception("E");
}

// autoload Elastica
call_user_func(function () {
  static $registered = false;
  if (!$registered) {
    $registered = true;
    spl_autoload_register(function ($class) {
      if (strpos($class, 'Elastica\\') == 0)
        require_once __DIR__ . '/externals/elastica/lib/' . str_replace('\\', '/', $class) . '.php';
    });
  }
});

class elasticSearch {
  
  protected static $instance;
  
  static function instance () {
    if (self::$instance === null)
      self::$instance = new self();
    return self::$instance;
  }
  
  protected $client;

  function __construct () {
    $this->client = new Elastica\Client();
  }
  
  function query ($query = array()) {
    return new elasticQuery($this->client, $query);
  }
  
}

class elasticQuery implements ArrayAccess, Iterator, Countable {

  protected $client;
  protected $query;

  function __construct ($client, $query) {
    $this->client = $client;
    $this->query = $query;
  }
  
  protected $buffer = array();
  protected $nextBufferOffset = 0;
  function ensureBufferData () {
    if (count($this->buffer) == 0) {
      $query = $this->query;
      $lowBound = (array_key_exists('limit', $query) && count($query['limit']) > 0 ? $query['limit'][0] : 0)
          + $this->nextBufferOffset;
      $length = min(16, count($this) - $this->nextBufferOffset);
      $query['limit'] = array($lowBound, $lowBound + $length);
      foreach (self::executeElasticSearch($query) as $item) {
        $this->buffer[$this->nextBufferOffset] = $item;
        $this->nextBufferOffset++;
      }
    }
  }
  
  function offsetExists ($offset) {
    assertTrue(false);
  }

  function offsetGet ($offset) {
    if (preg_match('/^\s*([0-9]+)\s*\.\.\s*(\$|[0-9]+)\s*$/', $offset, $match)) {
      version_assert and assertTrue($match[2] == '$' || $match[2] <= count($this));
      return $this->opSlice($match[1], $match[2]);
    }
    assertTrue(false);
  }

  function offsetSet ($offset, $value) {
    assertTrue(false);
  }

  function offsetUnset ($offset) {
    assertTrue(false);
  }
  
  function count () {
    version_assert and assertTrue(count(debug_backtrace()) < 1024);
    // todo: optimize
    return count(self::executeElasticSearch($this->query));
  }

  protected $position = 0;
  function rewind () {
    $this->position = 0;
  }

  function valid () {
    $this->ensureBufferData();
    return $this->position < count($this);
  }

  function key () {
    $keys = array_keys($this->buffer);
    return $keys[0];
  }

  function current () {
    return $this->buffer[$this->key()];
  }
  
  function next () {
    unset($this->buffer[$this->key()]);
    $this->position++;
  }
  
  function opSlice ($from, $to) {
    version_assert and assertTrue(count(func_get_args()) == 2);
    $query = $this->query;
    $offset = array_key_exists('limit', $query) && count($query['limit']) > 0 ? $query['limit'][0] : 0;
    $query['limit'] = array($offset + $from, $offset + ($to == '$' ? count($this) : $to));
    return new self($this->client, $query);
  }
  
  function executeElasticSearch ($query) {
  
    $search = new Elastica\Search($this->client);
    
    if (array_key_exists('select', $query)) {
      foreach ($query['select'] as $select) {
        version_assert and assertTrue(is_array($select) && count($select) == 1);
        if (key($select) == 'index')
          $search->addIndex(current($select));
        if (key($select) == 'type')
          $search->addType(current($select));
      }
    }
    
    $rawQuery = array('query' => array('match_all' => (object) array()));
    
    if (array_key_exists('where', $query))
      $rawQuery['query'] = self::parseWhere($query['where']);

    $elasticQuery = new Elastica\Query($rawQuery);
    
    if (array_key_exists('limit', $query) && count($query['limit']) > 0) {
      $elasticQuery->setFrom($query['limit'][0]);
      if ($query['limit'][1] != '$')
        $elasticQuery->setSize($query['limit'][1] - $query['limit'][0]);
    }

    return $search->search($elasticQuery);
  }

  static function parseWhere ($query) {
    version_assert and assertTrue(is_array($query));
    version_assert and assertTrue(count($query) <= 1);
    
    if (in_array('and', array_keys($query))) {
      version_assert and assertTrue(count(array_filter($query['and'], 'is_int')) == 0);
      return array(
        'bool' => array(
          'must' => array_map(array(__CLASS__, 'parseWhere'), $query['and']),
        ),
      );
    }

    if (in_array('or', array_keys($query))) {
      version_assert and assertTrue(count(array_filter($query['or'], 'is_int')) == 0);
      return array(
        'bool' => array(
          'should' => array_map(array(__CLASS__, 'parseWhere'), $query['or']),
        ),
      );
    }
    
    if (count($query) == 1) {
      $value = current($query);
      $operator = 'term';
      if (is_array($value)) {
        $operator = $value['operator'];
        $value = $value['value'];
      }
      return array($operator => array(key($query) => $value));
    }
      
    return array('match_all' => (object) array());
  }

}

//$e = new elasticSearch();

//$q = $e->query(array(
//  'id' => 'f2ce928b-67f2-40f2-8832-ad009f83da6d',
//));

//$q = $e->query(array('query' => array('bool' => array('must' => array(array('term' => array('id' => 'f2ce928b-67f2-40f2-8832-ad009f83da6d')))))));

//$q = $e->query(array('query' => array('term' => array('id' => 'f2ce928b-67f2-40f2-8832-ad009f83da6d'))));



//$q = $e->query(array('query' => array('term' => array('id' => 'f2ce928b-67f2-40f2-8832-ad009f83da6d'))));
//$q = $e->query();
//$q = $e->query(array('query' => array('match_all' => array())));


/*
$q = $e->query(array(
  'select' => array(
    array('index' => 'app-krk.internet-inovacije.com'),
    array('type' => 'object'),
  ),
  'where' => //array(
    //'or' => array(
      array('id' => array('operator' => 'term', 'value' => 'f2ce928b-67f2-40f2-8832-ad009f83da6d')),
    //  array('id' => 'b01f6194-a755-4f44-b42c-472362331409'),
    //),
  //),
));
/**/

/*
$q = $e->query(array(
  'select' => array(
    array('index' => 'app-krk.internet-inovacije.com'),
    array('type' => 'object'),
  ),
  'limit' => array(2, 6),
));
/**/

/*
var_dump(count($q));
//var_dump(count($q['2 .. 6']));

//var_dump(count($q['250 .. 253']));

//foreach ($q['20 .. 23'] as $r)
//foreach ($q['0 .. 10'] as $r)
//foreach ($q['2 .. 6'] as $r)

foreach ($q as $r)
  var_dump($r->id);

var_dump('-----');

//foreach ($q['1 .. 3'] as $r)
//  var_dump($r->id);
  //var_dump('.');

//var_dump($r["0..1"]);
/**/



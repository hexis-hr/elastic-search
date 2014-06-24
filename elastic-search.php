<?php

// require 'libraries/phops';

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

  function mapping ($mapping) {
    version_assert and assertTrue(is_array($mapping));

    if (count(array_filter($mapping, 'is_int')) == 0) {
      $indexes = array();
      foreach ($mapping as $mapping_) {
        if (is_string($mapping_['index']))
          $indexName = $mapping_['index'];
        else
          $indexName = $mapping_['index']['name'];

        version_assert and assertNotEqual($indexName, '');

        if (!array_key_exists($indexName, $indexes))
          $indexes[$indexName] = array(
            'types' => array(),
            'analysis' => array(),
          );

        if (array_key_exists('properties', $mapping_)) {
          $typeName = $mapping_['type'];
          $indexes[$indexName]['types'][$typeName] = array(
            'properties' => $mapping_['properties'],
          );
        }

        if (is_array($mapping_['index']) && array_key_exists('analysis', $mapping_['index']))
          foreach ($mapping_['index']['analysis'] as $analysisGroupName => $analysisGroup) {
            if (!array_key_exists($analysisGroupName, $indexes[$indexName]['analysis']))
              $indexes[$indexName]['analysis'][$analysisGroupName] = array();
            foreach ($analysisGroup as $analysisEntryName => $analysisEntry) {
              version_assert and assertTrue(!array_key_exists($analysisEntryName,
                $indexes[$indexName]['analysis'][$analysisGroupName]));
              $indexes[$indexName]['analysis'][$analysisGroupName][$analysisEntryName] = $analysisEntry;
            }
          }

      }
    }

    foreach ($indexes as $indexName => $indexMapping) {
      version_assert and assertNotEqual($indexName, '');
      $index = $this->client->getIndex($indexName);

      //if (false)
      if ($index->exists())
        $index->delete();

      if (!$index->exists())
        $index->create(array('analysis' => $indexMapping['analysis']));

      foreach ($indexMapping['types'] as $typeName => $typeMapping) {
        $elasticMapping = new \Elastica\Type\Mapping();
        $elasticMapping->setType($index->getType($typeName));
        $elasticMapping->setProperties($typeMapping['properties']);
        $elasticMapping->send();
      }

    }

  }

  /*
  function index ($name, $settings) {
    $index = $this->client->getIndex($name);
    //if ($index->exists())
      $index->delete();

    if (!$index->exists())
      $index->create($settings);
      //var_dump($settings);
      //exit;
    //$index->setSettings($settings);
  }
  /**/

  function set ($document) {
    $this->client
      ->getIndex($document['index'])
      ->getType($document['type'])
      ->addDocument(new \Elastica\Document($document['properties']['id'], $document['properties']))
    ;
  }

  function delete ($document) {
    $this->client
      ->getIndex($document['index'])
      ->getType($document['type'])
      ->deleteDocument(new \Elastica\Document($document['properties']['id'], $document['properties']))
    ;
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
    $this->ensureBufferData($offset);
    return $this->buffer[$offset];
  }

  function offsetSet ($offset, $value) {
    assertTrue(false);
  }

  function offsetUnset ($offset) {
    assertTrue(false);
  }

  function count () {
    version_assert and assertTrue(count(debug_backtrace()) < 1024);
    list($search, $elasticQuery) = $this->elasticBind($this->query);
    $count = $search->count($elasticQuery);
    if (array_key_exists('limit', $this->query) && count($this->query['limit']) > 0)
      $count = $this->query['limit'][1] - $this->query['limit'][0];
    return $count;
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
    version_assert and assertTrue(count($keys) > 0);
    return $keys[0];
  }

  function current () {
    return $this->buffer[$this->key()];
  }

  function next () {
    unset($this->buffer[$this->key()]);
    $this->position++;
  }

  function one () {
    $this->ensureBufferData();
    version_assert and assertTrue($this->valid() && count($this) == 1);
    return $this->current();
  }

  function opSlice ($from, $to) {
    version_assert and assertTrue(count(func_get_args()) == 2);
    $query = $this->query;
    $offset = array_key_exists('limit', $query) && count($query['limit']) > 0 ? $query['limit'][0] : 0;
    $query['limit'] = array($offset + $from, $offset + ($to == '$' ? count($this) : $to));
    return new self($this->client, $query);
  }

  function elasticBind ($query) {

    $search = new Elastica\Search($this->client);

    if (array_key_exists('select', $query)) {
      if (count($query['select']) == 1 && count(array_filter($query['select'], 'is_int')) == 0)
        $selects = array($query['select']);
      else
        $selects = $query['select'];
      foreach ($selects as $select) {
        version_assert and assertTrue(is_array($select) && count($select) == 1);
        version_assert and assertNotEqual(current($select), '');
        if (key($select) == 'index')
          $search->addIndex(current($select));
        if (key($select) == 'type')
          $search->addType(current($select));
      }
    }

    $rawQuery = array('query' => array('match_all' => (object) array()));

    if (array_key_exists('where', $query))
      $rawQuery['query'] = self::parseWhere($query['where']);

    if (array_key_exists('order', $query)) {
      version_assert and assertTrue(count(array_filter($query['order'], 'is_int')) == 0);
      $sort = array_merge($query['order'], array(array('score' => 'desc')));
      $rawQuery['sort'] = array();
      foreach ($sort as $k => $v) {
        version_assert and assertTrue(is_int($k));
        version_assert and assertTrue(is_string($v) || count($v) == 1);
        if (is_string($v) && $v == 'score')
          $rawQuery['sort'][] = '_score';
        else if (is_array($v) && is_string(key($v)) && key($v) == 'score')
          $rawQuery['sort'][] = array('_score' => current($v));
        else if (is_string($v) && preg_match('/[\*\s]/', $v))
          $rawQuery['query'] = array('function_score' => array(
            'query' => $rawQuery['query'],
            'functions' => array(array('script_score' => array('script' => $v))),
          ));
        else
          $rawQuery['sort'][] = $v;
      }
    }

    return array($search, new Elastica\Query($rawQuery));
  }

  function executeElasticSearch ($query) {

    list($search, $elasticQuery) = $this->elasticBind($query);

    // overwrite default limit
    $elasticQuery->setSize($search->count($elasticQuery));

    if (array_key_exists('limit', $query) && count($query['limit']) > 0) {
      $elasticQuery->setFrom($query['limit'][0]);
      if ($query['limit'][1] != '$')
        $elasticQuery->setSize($query['limit'][1] - $query['limit'][0]);
    }

    return $search->search($elasticQuery);
  }

  function elasticQuery () {
    return self::parseWhere($this->query['where']);
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
        unset($value['operator']);
        //$fieldValue = $value['value'];
        //unset($value['value']);
        //$value[key($query)] = $fieldValue;
        //$value = $value['value'];
      }

      if (in_array($operator, array('term', 'text')) && is_array($value)) {
        version_assert and assertTrue(count($value) == 1);
        $value = $value['value'];
      }

      if ($operator == 'string') {
        return array(
          'query_string' => array(
            'fields' => array(key($query)),
            'query' => $value['value'],
          ),
        );
      }

      if ($operator == 'fuzzy_like_this') {
        $value['like_text'] = $value['value'];
        unset($value['value']);
        $value['fields'] = array(key($query));
        return array('fuzzy_like_this' => $value);
        /*
        return array(
          'fuzzy_like_this' => array(
            'fields' => array(key($query)),
            'like_text' => $value['value'],
          ),
        );
        /**/
      }

      //if ($operator == 'fuzzy') {
      //  return array($operator => array(key($query) => $value));
      //}

      //if (is_array($value) && count($value) == 1) {
      //if (is_string($value))
      //  return array('term' => array(key($query) => $value));

      //if (is_array($value) && count($value) == 2)
      //  return array($value['operator'] => array(key($query) => $value['value']));

      //assertTrue(false);

      return array($operator => array(key($query) => $value));
      //return array($operator => $value);
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



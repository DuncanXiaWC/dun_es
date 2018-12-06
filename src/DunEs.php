<?php
/**
 * Created by DuncanXia.
 * Date: 2018/11/27
 */

namespace duncanxia\DunEs;


class DunEs
{
    protected $es;

    protected $params = [];
    protected $paramsBody = [
        'query'=>['bool'=>[]]
    ];
    protected $paramsBodyQuery = [
        'must'=>[],
        'must_not'=>[],
        'should'=>[]
    ];
    public function __construct()
    {
        $this->es = \Elasticsearch\ClientBuilder::create()->build();
    }

    /**
     * Created by DuncanXia.
     * Date: 2018/11/27
     */
    public function create(string $index){
        return $this->es->indices()->create([
            'index'=>$index,
            'body'=>[
                'settings' => [
                    'number_of_shards' => 5,
                    'number_of_replicas' => 0,
                    'refresh_interval' => -1
                ]
            ]
        ]);
    }

    /**
     * Created by DuncanXia.
     * Date: 2018/11/27
     */
    public function delete(string $index){
        return $this->es->indices()->delete(['index'=>$index]);
    }

    public function deleteOne(string $index, string $type, $id){
        return $this->es->delete(['index'=>$index,'type'=>$type,'id'=>$id]);
    }

    /**
     * Created by DuncanXia.
     * Date: 2018/11/28
     */
    public function insert(string $index, string $type, array $data){
        foreach ($data as &$datum){
            $datum = trim($datum);
            if ($datum == '')
                $datum = null;
//            if(preg_match('/^\d+$/',$datum))
//                $datum = (int)$datum;
        }
        $params = [
            'index'=>$index,
            'type'=>$type,
            'body'=>$data
        ];
//        var_dump($params);
        return $this->es->index($params)['_id'];
    }

    /**
     * Created by DuncanXia.
     * Date: 2018/11/27
     */
    public function put(string $index, string $type, array $data){
        $ids = [];
        $row_num = [];
        foreach ($data as $datum){
            foreach ($datum as &$item){
                if(trim($item) == '')
                    $item = null;
            }
            $params = [
                'index'=>$index,
                'type'=>$type,
                'body'=>$datum
            ];
            $row_num[] = $datum['ROW_NUMBER'];
            $ids[] = $this->es->index($params)['_id'];
//            var_dump(sizeof($ids));
        }

        return array_combine($row_num,$ids);
    }

    /**
     * Created by DuncanXia.
     * Date: 2018/11/27
     */
    public function db(string $index, string $type){
        $this->params = [
            'index'=>$index,
            'type'=>$type
        ];
        return $this;
    }

    /**
     * Created by DuncanXia.
     * Date: 2018/11/29
     */
    public function limit(int $offset, int $limit){
        $this->paramsBody['from'] = $offset;
        $this->paramsBody['size'] = $limit;
        return $this;
    }

    /**
     * Created by DuncanXia.
     * Date: 2018/11/29
     */
    public function order(string $field, string $order){
        $this->paramsBody['sort'] = [$field=>['order'=>$order]];
        try{
            $this->get();
        }catch (\Exception $exception){
            $this->paramsBody['sort'] = [$field.'.keyword'=>['order'=>$order]];
        }
        return $this;
    }
    /**
     * Created by DuncanXia.
     * Date: 2018/11/27
     * $search = ['field'=>'field','op'=>'op','command'=>'command']
     * $order = ['rq','desc']
     */
    public function where(string $field, string $op, $command, string $condition='must'){
        switch (trim($op)){
            case 'like':
                if (is_string($command))
                    $command = trim($command);
                $this->paramsBodyQuery[$condition][] = [
                    'wildcard' => [
                        $field => $command
                    ]
                ];
                break;
            case 'between':
                $this->paramsBodyQuery[$condition][] = [
                    'range' => [
                        $field => [
                            'gte'=> $command[0],
                            'lte'=> $command[1]
                        ]
                    ]
                ];
                break;
            case '=':
            case 'eq':
                if (is_string($command))
                    $command = trim($command);
                $this->paramsBodyQuery[$condition][] = [
                    'term' => [
                        $field => $command,
                    ]
                ];
                break;
            case 'in':
                $data = $this->get()['data'];
                $or_field = explode('.',$field)[0];
                if (!array_key_exists($or_field, $data[0]))
                    break;
                foreach ($command as &$item)
                    if(is_string($item))
                        $item = trim($item);
                $this->paramsBodyQuery[$condition][] = [
                    'terms'=>[
                        $field=>$command
                    ]
                ];
                break;
        }
        return $this;
    }

    /**
     * Created by DuncanXia.
     * Date: 2018/11/27
     */
    public function whereNot(string $field, string $op, $command,int $boost=1){
        return $this->where($field, $op, $command, $boost, 'must_not');
    }

    /**
     * Created by DuncanXia.
     * Date: 2018/11/27
     */
    public function whereOr(string $field, string $op, $command,int $boost=1){
        return $this->where($field, $op, $command, $boost, 'should');
    }

    /**
     * Created by DuncanXia.
     * Date: 2018/11/27
     */
    public function field(array $fields){
        foreach ($fields as $field)
            $this->paramsBody['_source'][] = $field;
        return $this;
    }

    /**
     * 获取数据
     * Created by DuncanXia.
     * Date: 2018/11/27
     */
    public function get(bool $flag=false){
        $this->mixQuery();
        $res = $this->es->search($this->params);
        if ($flag)
            return $res;
        $count = $res['hits']['total'];
        $data = array_column($res['hits']['hits'],'_source');
        return ['data'=>$data,'count'=>$count];
    }

    /**
     * es聚合sum
     * Created by DuncanXia.
     * Date: 2018/11/29
     */
    public function sum($field){
        $params = $this->params;
        if (is_array($field))
        {
            $data = [];
            foreach ($field as $item){
                $params['body'] = [
                    'aggs'=> [
                        'return_expires_in'=>[
                            'sum'=>[
                                'field'=>$item
//                                ,'fielddata'=>true
                            ]
                        ]
                    ],
                    'size'=>0
                ];
                try{
                    $val = $this->es->search($params)['aggregations']['return_expires_in'];
                    $data[$item] = number_format($val['value'],6);
                    if (isset($val['value_as_string']))
                        $data[$item] = 0;
                }catch (\Exception $exception){
                    $data[$item] = 0;
                }
            }
        }else{
            $params['body'] = [
                'aggs'=> [
                    'return_expires_in'=>[
                        'sum'=>[
                            'field'=>$field
//                            ,'fielddata'=>true
                        ]
                    ]
                ],
                'size'=>0
            ];
            try{
                $val = $this->es->search($params)['aggregations']['return_expires_in'];
                $data = number_format($val['value'], 6);
                if (isset($val['value_as_string']))
                    $data = 0;
            }catch (\Exception $exception){
                $data = 0;
            }
        }
        return $data;
    }

    /**
     * 在当前查询结果里面计算
     * Created by DuncanXia.
     * Date: 2018/11/27
     */
    public function _sum($field){
        $start = 1;
        $limit = 2000;
        if(is_array($field))
        {
            $sum = [];
            foreach ($field as $item)
                $sum[$item] = 0;
        }
        else
            $sum = 0;
        while (true){
            $data = $this->limit(($start-1)*$limit,$limit)->get()['data'];
            $start++;
            if (empty($data))
                break;
            if (is_array($field)){
                foreach ($field as $item){
                    $column = array_column($data, $item);
                    $sum[$item] += array_sum($column);
                }
            }else{
                $column = array_column($data, $field);
                $sum += array_sum($column);
            }
        }
        return $sum;
    }


    /**
     * Created by DuncanXia.
     * Date: 2018/11/28
     */
    public function update(string $id,array $update){
        $params = $this->params;
        $params['id'] = $id;
        if (!$this->es->exists($params))
            return false;
        $params['body'] = [
            'doc'=>$update
        ];
        $this->es->update($params);
        unset($params['body']);
        return $this->es->get($params);
    }

    /**
     * Created by DuncanXia.
     * Date: 2018/11/28
     */
    public function updates(array $data){
        $params = $this->params;
        $newInsert = [];
        foreach ($data as $datum){
            $params['id'] = $datum['_id'];
            if (!$this->es->exists($params))
                $newInsert[] = $this->put($this->params['index'],$this->params['type'],$datum['data']);
            $params['body'] = [
                'doc'=>$datum['data']
            ];
            $this->es->update($params);
//            var_dump($datum['_id']);
        }
        return $newInsert;
    }


    /**
     * 合成query
     * Created by DuncanXia.
     * Date: 2018/11/27
     */
    private function mixQuery(){
//        if (isset($this->params['body']))
//            return;
        if (empty($this->paramsBodyQuery['must'])
            && empty($this->paramsBodyQuery['must_not'])
            && empty($this->paramsBodyQuery['should'])
        )
            $this->paramsBody['query']['bool'] = ['match_all'=>[]];
        else
            $this->paramsBody['query']['bool'] = $this->paramsBodyQuery;
        $this->params['body'] = $this->paramsBody;
    }

    /**
     * 获取查询语句
     * Created by DuncanXia.
     * Date: 2018/11/27
     */
    public function getQuery(){
        $this->mixQuery();
        return $this->params;
    }

    public function getEs(){
        return $this->es;
    }

    public function exists(string $index, string $type, $id=null){
        if ($id != null)
            return $this->es->indices()->exists(['index'=>$index]);
        else
            return $this->es->exists(['index'=>$index,'type'=>$type,'id'=>$id]);

    }

    public function bulk(string $typeFlag='create', array &$data, string $index, string $type='test', string $id='_id'){
        $params['body'] = [];
        foreach ($data as $datum){
            if (!isset($datum[$id]))
                return false;
            $params['body'][] = [
                $typeFlag=>[
                    '_index' => $index,
                    '_type' => $type,
                    '_id' => $datum[$id]
                ]
            ];
            unset($datum[$id]);
            if ($typeFlag == 'create')
                $params['body'][] = $datum;
            elseif($typeFlag == 'update')
                $params['body'][] = [
                    'doc'=>$datum
                ];
        }
//        ajax_return($data);
        return $this->es->bulk($params);
    }
}
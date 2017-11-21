const MongoClient = require('mongodb').MongoClient


const db_connection = 'mongodb://localhost:27017/douma_production'

const delete_duplicates_except_last = async () => {
  const db = await MongoClient.connect(db_connection)
  const records = db.collection('records')
  
  const count = await records.count()

  const duplicate_groups = await records.aggregate([
    {'$group': {'_id': '$id', 'count': {'$sum': 1}}},
    {'$match': {'_id': {'$ne': null}, 'count': {'$gt': 1}}},
    {'$sort': {'count': -1}},
  ]).toArray()

  const duplicated_ids = duplicate_groups.map(d => d._id)

  duplicated_ids.forEach(async (id) => {
    const duplicated_records = await records.find({id: id}).toArray()
    const count = duplicated_records.length

    if (count > 1) {
      const all_ids = duplicated_records.map(d => d._id)
      const ids_to_delete= all_ids.slice(0, -1)
      // console.log(ids_to_delete)
      const result = await records.deleteMany({_id: {$in: ids_to_delete}})
      //
      // const result = await records.aggregate([
      //   {$match: {_id: {$in: ids_to_delete}}},
      //   {$out: 'new_collection'}
      // ]).toArray()

      console.log(result)
    } else {
      console.log('scream!')
    }
  })

}

delete_duplicates_except_last()
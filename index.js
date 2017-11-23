const md5 = require('md5')
const MongoClient = require('mongodb').MongoClient
const Logger = require('./logger').Logger
const logger = new Logger()
let BATCH_SIZE = 100
let DRY_RUN = true
const db_connection = 'mongodb://localhost:27017/douma_production'

const run = async () => {
  let records_initial_count = 0 // done
  let records_groups_with_duplicates = 0
  let records_all_duplicates = 0
  let records_plan_to_move = 0
  let records_plan_to_remain = 0
  let records_moved = 0
  let records_expected_remain_after_move= 0
  let records_diff_expected_actual_end = 0
  let records_actual_end_count = 0
  let records_duplicate_ids = 0
  
  const db = await MongoClient.connect(db_connection)
  const records = db.collection('records')
  const duplicate_id_records = db.collection('duplicate_id_records')
  
  records_initial_count = await records.count()

  const find_duplicates = async () => {

    const count = await records.count()

    const duplicate_groups = await records.aggregate([
      {'$group': {'_id': '$id', 'count': {'$sum': 1}}},
      {'$match': {'_id': {'$ne': null}, 'count': {'$gt': 1}}},
      {'$sort': {'count': -1}},
    ]).toArray()

    // logger.write({group_count: duplicate_groups.length, duplicate_groups})
    return duplicate_groups
}

  const count_duplicate_records = (duplicate_groups) => {
    let number_of_groups_with_more_than_1_record = 0

    const count_of_duplicate_records = duplicate_groups.reduce((acc, group) => {
      if (group.count > 1) number_of_groups_with_more_than_1_record += 1
      acc += group.count
      return acc
    }, 0)

    records_groups_with_duplicates = number_of_groups_with_more_than_1_record
    records_all_duplicates = count_of_duplicate_records

    const records_plan_to_move = count_of_duplicate_records - number_of_groups_with_more_than_1_record

    logger.write({
      number_of_groups: number_of_groups_with_more_than_1_record,
      duplicate_records_count: count_of_duplicate_records,
      records_plan_to_move: records_plan_to_move
    })

    return records_plan_to_move
  }

  // Take a group {id, count}
  // Return
  const count_exact_duplicates_in_group = async (group) => {
    let unique_hashes = []

    const group_records = await records.find({id: group._id}).toArray()
    group_records.forEach(record => {
      delete record.updated_at
      delete record._id
      const string = JSON.stringify(record)
      const hash = md5(string)
      if (!unique_hashes.includes(hash)) unique_hashes.push(hash)
    })

    const result = {group_id: group._id, unique_hashes_length: unique_hashes.length}
    return result
  }

  const move_everything_but_latest_in_group = async (group) => {
    let output = []
    const group_records = await records.find({id: group._id}).sort({_id: -1}).toArray()

    const records_keep = group_records.slice(0,1)
    const records_move = group_records.slice(1)

    if (records_keep.length !== 1) {
      throw new Error('somehow - who knows - nicolai was wrong. there are no records to keep')
    } else if (records_move.length < 1) {
      throw new Error('somehow - who knows - nicolai was wrong. there are no records to move')
    } else {
      // move everything in move
      const ids_to_move = records_move.map(r => r._id)
      const ids_strings = ids_to_move.map(id => id.toString())
      logger.write({move_ids: ids_strings, move_ids_count: ids_strings.length, duplicate_record_id: group._id})

      // console.log('group._id', group._id)
      // console.log('records_keep.length', records_keep.length)
      // console.log('records_move.length', records_move.length)

      try {
        await duplicate_id_records.insertMany(records_move)
        const result = await records.deleteMany({_id: {$in: ids_to_move}})
        if (result.deletedCount !== records_move.length) {
          throw new Error('Stop, have deleted wrong number of records')
        }
      } catch (e) {
        console.log(e || 'Something went wrong inserting into collection duplicate_id_records')
        process.exit()
      }

      return ids_strings.length
    }
  }


    // Pipeline
  // 1. Find duplicates
  const duplicate_groups = await find_duplicates()
  records_plan_to_move = count_duplicate_records(duplicate_groups)

  // 2. Count exact duplicates in group
  // const some_result = duplicate_groups//.slice(0, 10)
  // const another_result = some_result.map((i) => {
  //   return count_exact_duplicates_in_group(i)
  // })
  // const result = await Promise.all(another_result)
  // logger.write({result})

  // 3. Move everything but latest in each group
  if (!DRY_RUN) {
    const promises = duplicate_groups.slice(0,BATCH_SIZE).map(async (group) => {
      return await move_everything_but_latest_in_group(group)
    })
    const result = await Promise.all(promises)
    records_moved = result.reduce((acc, i) => acc + i,0 )
  }

  console.log("\nPRE-FIX STATS\n")
  console.log('records_initial_count',records_initial_count)
  console.log('records_groups_with_duplicates', records_groups_with_duplicates)
  console.log('records_all_duplicates', records_all_duplicates)
  console.log('records_plan_to_move',records_plan_to_move)
  records_plan_to_remain = records_initial_count - records_plan_to_move
  console.log('records_plan_to_remain', records_plan_to_remain)
  console.log('records_moved',records_moved)

  console.log("\nPOST-FIX STATS\n")
  records_expected_remain_after_move= records_initial_count - records_moved
  console.log('records_expected_remain_after_move',records_expected_remain_after_move)
  records_actual_end_count = await records.count()
  console.log('records_actual_end_count',records_actual_end_count)
  records_diff_expected_actual_end = records_actual_end_count - records_expected_remain_after_move
  console.log('records_diff_expected_actual_end', records_diff_expected_actual_end)
  const moved_correct_number = records_expected_remain_after_move=== records_actual_end_count
  console.log('moved_correct_number', moved_correct_number)
  records_duplicate_ids = await duplicate_id_records.count()
  console.log('records_duplicate_ids', records_duplicate_ids)  
}

BATCH_SIZE = 10000
DRY_RUN = false
run().then(process.exit)

package com.timeout
import com.amazonaws.services.kinesis.model.{Shard => AwsShard, _}


/**
  * A node in a tree of shards
  */
private [timeout] case class Shard(id: String, children: List[Shard])

object Shard {

  /**
    * Given a shard, and a list of AWS shards
    * produce a graph such that parent shards will be linked to their children in the
    * case of a shard being split
    */
  private def toShard(s: AwsShard, otherShards: List[AwsShard]): Shard = {
    val directChildren = otherShards.filter(_.getParentShardId == s.getShardId)
    Shard(
      id = s.getShardId,
      children = directChildren.map(toShard(_, otherShards))
    )
  }

  /**
    * We want to enforce that the shard ID in getParentShardId actually exists,
    * if it doesn't we'll make it null (sorry) to match what the Java API returns
    * in the case of a shard not having a parent.
    */
  private def removePhantomParents(shards: List[AwsShard]): List[AwsShard] = {
    val shardIds = shards.map(_.getShardId).toSet
    shards.map {
      case s if !shardIds.contains(s.getParentShardId) => s.withParentShardId(null)
      case s => s
    }
  }

  /**
    * Create a list of shards from a list of AWS shards
    */
  def fromAws(awsShards: List[AwsShard]): List[Shard] = {
    val (parents, children) = removePhantomParents(awsShards)
      .partition(_.getParentShardId == null)
    parents.map(toShard(_, children))
  }
}

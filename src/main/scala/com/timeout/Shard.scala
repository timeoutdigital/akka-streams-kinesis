package com.timeout
import com.amazonaws.services.kinesis.model.{Shard => AwsShard, _}


/**
  * A node in a graph of shards
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
    * Create a list of shards from a list of AWS shards
    */
  def fromAws(awsShards: List[AwsShard]): List[Shard] = {
    val (parents, children) = awsShards.partition(_.getParentShardId == null)
    parents.map(toShard(_, children))
  }
}

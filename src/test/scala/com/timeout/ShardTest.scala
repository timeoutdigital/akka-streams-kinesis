package com.timeout

import com.amazonaws.services.kinesis.model.{Shard => AwsShard, _}
import org.scalatest.{FreeSpec, Matchers}

class ShardTest extends FreeSpec with Matchers {

  "Shard fromAws method" - {

    "Work normally for three top level shards" in {

      val shards = List(
         new AwsShard().withShardId("foo1"),
         new AwsShard().withShardId("foo2"),
         new AwsShard().withShardId("foo3")
      )

      Shard.fromAws(shards) shouldEqual List(
        Shard("foo1", List.empty),
        Shard("foo2", List.empty),
        Shard("foo3", List.empty)
      )
    }

    "Turn child shards into embedded lists" in {
       val shards = List(
         new AwsShard().withShardId("foo1"),
         new AwsShard().withShardId("foo2"),
         new AwsShard().withShardId("foo1-child1").withParentShardId("foo1"),
         new AwsShard().withShardId("foo1-child2").withParentShardId("foo1"),
         new AwsShard().withShardId("foo2-child").withParentShardId("foo2")
      )

      Shard.fromAws(shards) shouldEqual List(
        Shard("foo1", List(Shard("foo1-child1", List.empty), Shard("foo1-child2", List.empty))),
        Shard("foo2", List(Shard("foo2-child", List.empty)))
      )
    }

    "Only include one link from a parent to a child in the case of a stream merge" in {
        val shards = List(
         new AwsShard().withShardId("foo1"),
         new AwsShard().withShardId("foo1-child1").withParentShardId("foo1"),
         new AwsShard().withShardId("foo1-child2").withParentShardId("foo1"),
         new AwsShard().withShardId("foo2")
           .withParentShardId("foo1-child1")
           .withAdjacentParentShardId("foo1-child2")
      )

      Shard.fromAws(shards) shouldEqual List(
        Shard("foo1", List(
          Shard("foo1-child1", List(
            Shard("foo2", List.empty)
          )),
          Shard("foo1-child2", List.empty)
        ))
      )
    }

    "Handle the situation where the parent streams have gone" in {
      val shards = List(
         new AwsShard().withShardId("foo1-child1").withParentShardId("foo1"),
         new AwsShard().withShardId("foo1-child2").withParentShardId("foo1")
      )
      Shard.fromAws(shards) shouldEqual List(
        Shard("foo1-child1", List.empty),
        Shard("foo1-child2", List.empty)
      )
    }
  }
}

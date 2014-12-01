/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.streaming.scheduler

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import com.typesafe.config.ConfigFactory
import scala.collection.mutable.HashMap;

import akka.remote.DisassociatedEvent;
import akka.actor._;
import org.apache.spark.util._;
import org.apache.spark._;
import org.apache.spark.rdd._;
import org.apache.spark.streaming.scheduler._;
import org.apache.spark.scheduler.cluster._;
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages._;
import System.out._;
import scala.math._;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.spark.util._;

class RemoteActor(hash : HashMap[Int, ActorRef]) extends Actor {
     var sparkActorSystem: ActorSystem = null;
     var boundPort: Int = 0;
     var seen: Boolean = false;

     //val clientSystem = ActorSystem("client", ConfigFactory.load(ConfigFactory.parseString("""
     //  akka {
     //    actor {
     //      provider = "akka.remote.RemoteActorRefProvider"
     //
     //      remote {
     //        enabled-transports = ["akka.remote.netty.tcp"]
     //        netty.tcp {
     //          hostname = "127.0.0.1"
     //          port = 0
     //        }
     //      }
     //   }
     // } """)))

  def receive = {
    // props
    case msg: DriverConfMessage =>
              if (seen == false) {
                seen = true;
                println("Received DriverConfMessage");
                println("Configuration: " + msg.conf);
                val driverConf = new SparkConf().setAll(msg.conf)
                val (system, bport) = AkkaUtils.createActorSystem(
                    "frontend", "f16", 0, driverConf, new SecurityManager(driverConf))
                sparkActorSystem = system;
                boundPort = bport; 
              }
              sender ! "Executor, this is the scheduler";
    case DisassociatedEvent(_, address, _) =>
              println("Disassociated. Address: " + address);
    case msg: RegisterExecutorScheduler =>
              println(s"Received RegisterExecutorScheduler $msg " + msg.executorId)
              //val address = msg.executorAddr + "/user/Executor";
              //val actor = clientSystem.actorSelection(address);
              //val actor = sparkActorSystem.actorSelection(address);
              println("Created actor: " + sender.path.address.toString);
              sender ! "Executor, this is the scheduler";
              hash += (msg.executorId.toInt -> sender);
    case msg: LaunchTaskDecentralized =>
              println("Received LaunchTaskDecentralized (" + msg.executorId + ", task)");
              //val actor = hash(min(1,msg.executorId.toInt + 1))
              if (hash.size > 0) {
                val actor = hash(msg.executorId.toInt);
                println(s"Sending task to executor " + msg.executorId)
                actor ! LaunchTask(msg.data)
                println("Task sent")
              } else {
                println("No executors in Hash")
              }
    case msg: String =>
        println(s"RemoteActor received string '$msg'")
    case jobSet: JobSet =>
        println(s"RemoteActor received jobSet")
    case rdd: RDD[_] =>
        println(s"RemoteActor received RDD")
    case job: Job =>
        println(s"RemoteActor received job")
    case _ => System.out.println("!!!!! Unknown msg. !!!!"); 
              System.exit(-1)
  }
}

object Frontend {
// our actor
//val system = ActorSystem("RemoteSystem")
//private val SchedulerActorSupervisor = system.actorOf(Props[RemoteActor], "RemoteActor1");

def main(args: Array[String]) {

     //val conf = new SparkConf;
     //val securityManager = new SecurityManager(conf);
     
     //val (actorSystem, boundPort) = AkkaUtils.createActorSystem("server", 
     //                              "localhost", 8338, conf,  securityManager);
     
     //val SchedulerActorSupervisor = actorSystem.actorOf(Props[RemoteActor], "server");
     
     //val serverSystem = ActorSystem("server", ConfigFactory.load(ConfigFactory.parseString("""
     //   akka {
     //     event-handlers = ["akka.event.Logging$DefaultLogger"]
     //     loglevel = "DEBUG"
     //     actor {
     //       provider = "akka.remote.RemoteActorRefProvider"
     //     }

     //     failure-detector {
     //       threshold = 1800.0
     //       acceptable-heartbeat-pause = 6000s
     //       heartbeat-interval = 500s
     //       heartbeat-request {
     //         expected-response-after = 2000s
     //       }
     //     }
     //     debug {
     //       receive = on
     //       autoreceive = on
     //     }
     //     remote {
     //       log-sent-messages = on
     //       log-received-messages = on
     //       enabled-transports = ["akka.remote.netty.tcp"]
     //       netty.tcp {
     //         hostname = "f16"
     //         port = 8338
     //       }
     //     }
     //   }
     //""")))
    

    //      failure-detector {
    //        heartbeat-interval = 10000s
    //        acceptable-heartbeat-pause = 30000s
    //        threshold = 10000s
    //      }
    //      heartbeat-request {
    //        # Grace period until an explicit heartbeat request is sent
    //        grace-period = 10 s
 
    //        # After the heartbeat request has been sent the first failure detection
    //        # will start after this period, even though no heartbeat mesage has
    //        # been received.
    //        expected-response-after = 10 s
 
    //        # Cleanup of obsolete heartbeat requests
    //        time-to-live = 60 s
    //      }


 
     //val clientSystem = ActorSystem("client", ConfigFactory.load(ConfigFactory.parseString("""
     //  akka {
     //    actor {
     //      provider = "akka.remote.RemoteActorRefProvider"
     //
     //      remote {
     //        enabled-transports = ["akka.remote.netty.tcp"]
     //        netty.tcp {
     //          hostname = "127.0.0.1"
     //          port = 0
     //        }
     //      }
     //   }
     // } """)))
     val nodeHash = new HashMap[Int, ActorSelection];
     //
     //(1 to 15).foreach( node => {
     //       val workerAddr = "akka.tcp://sparkExecutor@f" + 
     //                          node.toString + ":6666/user/Executor";
     //       val actor = clientSystem.actorSelection(workerAddr);
     //       nodeHash += (node -> actor);
     //    });

     val executorConf = new SparkConf;
     val (serverSystem,_) = AkkaUtils.createActorSystem("server", "f16", 
                        8338, executorConf, new SecurityManager(executorConf))
     val server = serverSystem.actorOf(Props(classOf[RemoteActor],nodeHash), name = "server")
    
//     serverSystem.awaitTermination(); 
     Thread.sleep(10000000L)
  }
}


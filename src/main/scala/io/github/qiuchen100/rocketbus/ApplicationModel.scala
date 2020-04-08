package io.github.qiuchen100.rocketbus

import io.github.qiuchen100.rocketbus.api._
import scala.collection.mutable.ListBuffer

/**
 * @Author: github.com/qiuchen100
 * @Description: TODO
 * @Date: 2020/3/24 0:08
 * @Modified By:
 **/
class ApplicationModel {
  var startProcess : AbstractStart = _

  var inputProcess : List[AbstractInput] = _

  var computeProcess : List[AbstractCompute] = _

  var outputProcess : List[AbstractOutput] = _

  var endProcess : AbstractEnd = _

  var dagList : ListBuffer[ListBuffer[Process]] = _

  def dagProcessNames = {
    dagList.flatMap(_.toSeq)
      .map(process => process.getProcessName)
      .toSet
  }

  def putDAGProcess(processList : ListBuffer[Process]) = {
    while (!processList.isEmpty) {
      val listBuffer = ListBuffer[Process]()
      for (process <- computeProcess) {
        if (process.getDependencies.toSet.subsetOf(dagProcessNames)){
          listBuffer.append(process)
        }
      }
      dagList.append(listBuffer)
      processList --= listBuffer
    }
  }

  def CreateDAG : ListBuffer[ListBuffer[Process]] = {

    dagList.append(ListBuffer(startProcess))
    dagList.append(ListBuffer(inputProcess : _*))
    putDAGProcess(ListBuffer(computeProcess : _*))
    putDAGProcess(ListBuffer(outputProcess : _*))
    dagList.append(ListBuffer(endProcess))
    dagList
  }
}

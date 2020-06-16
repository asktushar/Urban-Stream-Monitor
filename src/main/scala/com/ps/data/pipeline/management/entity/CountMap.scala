package com.ps.data.pipeline.management.entity

case class CountMap (
                      currentCount: Long,
                      restartCount: Long,
                      failCount: Long,
                      lastRestartStatusRestart: Boolean,
                      lastRestartStatusFail: Boolean
                    )

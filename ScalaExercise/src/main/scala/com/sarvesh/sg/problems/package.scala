package com.sarvesh.sg

/**
 * 
 * Package object is used to store the utility functions which are used in other objects as well.
 * 
 * */

package object problems {
  def isAllDigits(x: String) = x.forall(_.isDigit)
}
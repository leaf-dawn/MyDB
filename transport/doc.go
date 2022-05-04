package transport

/**
* 写一下设计思路:
* packager 用于发送和接收 package 获取到的数据直接可以用于应用
* package是报文格式
* packager是把package根据一定的协议打包成[]byte
* transporter用于接收和发送package,用packager来经行转换
*
 */

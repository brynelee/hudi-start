package com.xdorg1.hudimomo

case class MomoMessage(
    msg_time: String,
    sender_nickyname: String,
    sender_account: String,
    sender_sex: String,
    sender_ip: String,
    sender_os: String,
    sender_phone_type: String,
    sender_network: String,
    sender_gps: String,
    receiver_nickyname: String,
    receiver_ip: String,
    receiver_account: String,
    receiver_os: String,
    receiver_phone_type: String,
    receiver_network: String,
    receiver_gps: String,
    receiver_sex: String,
    msg_type: String,
    distance: String,
    message: String
)

create table if not exists seckill_voucher (
    voucher_id bigint primary key,
    stock int not null
);

create table if not exists voucher_order (
    order_id varchar(64) primary key,
    user_id varchar(64) not null,
    voucher_id bigint not null,
    created_at timestamp not null,
    key idx_voucher_order_user_voucher (user_id, voucher_id)
);

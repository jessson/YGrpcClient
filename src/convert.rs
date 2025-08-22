use std::fmt;

use solana_sdk::{account::Account, pubkey::Pubkey, signature::Signature};
use solana_transaction_status::{EncodedTransactionWithStatusMeta, TransactionWithStatusMeta, UiTransactionEncoding};
use yellowstone_grpc_proto::geyser::{SubscribeRequestFilterAccounts, SubscribeUpdateAccount, SubscribeUpdateAccountInfo, SubscribeUpdateTransaction};


#[allow(dead_code)]
pub struct TransactionPretty {
    pub slot: u64,
    pub signature: Signature,
    pub is_vote: bool,
    pub tx: TransactionWithStatusMeta,
}
impl fmt::Debug for TransactionPretty {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        struct TxWrap<'a>(&'a EncodedTransactionWithStatusMeta);
        impl<'a> fmt::Debug for TxWrap<'a> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                let serialized = serde_json::to_string(self.0).expect("failed to serialize");
                fmt::Display::fmt(&serialized, f)
            }
        }

        f.debug_struct("TransactionPretty")
            .field("slot", &self.slot)
            .field("signature", &self.signature)
            .field("is_vote", &self.is_vote)
            .field(
                "tx",
                &TxWrap(
                    &self
                        .tx
                        .clone()
                        .encode(UiTransactionEncoding::Base64, Some(u8::MAX), true)
                        .expect("failed to encode"),
                ),
            )
            .finish()
    }
}

impl From<SubscribeUpdateTransaction> for TransactionPretty {
    fn from(SubscribeUpdateTransaction { transaction, slot }: SubscribeUpdateTransaction) -> Self {
        let tx = transaction.expect("should be defined");
        Self {
            slot,
            signature: Signature::try_from(tx.signature.as_slice()).expect("valid signature"),
            is_vote: tx.is_vote,
            tx: yellowstone_grpc_proto::convert_from::create_tx_with_meta(tx)
                .expect("valid tx with meta"),
        }
    }
}




#[allow(dead_code)]
pub struct AccountInfoPretty {
    pub slot: u64,
    pub pubkey: Pubkey,
    pub is_startup: bool,
    pub account: Account,
}

impl fmt::Debug for AccountInfoPretty {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        struct AccountWrap<'a>(&'a Account);
        impl<'a> fmt::Debug for AccountWrap<'a> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                let serialized = serde_json::to_string(self.0).expect("failed to serialize");
                fmt::Display::fmt(&serialized, f)
            }
        }

        f.debug_struct("AccountInfoPretty")
            .field("slot", &self.slot)
            .field("pubkey", &self.pubkey)
            .field("is_startup", &self.is_startup)
            .field(
                "account",
                &AccountWrap(
                    &self
                        .account
                        .clone()
                ),
            )
            .finish()
    }
}

impl From<SubscribeUpdateAccount> for AccountInfoPretty {
    fn from(SubscribeUpdateAccount { account, slot , is_startup}: SubscribeUpdateAccount) -> Self {
        let account = account.expect("should be defined");
        let (pubkey, account) = yellowstone_grpc_proto::convert_from::create_account(account).expect("valid account");
        Self {
            slot: slot.clone(),
            pubkey,
            is_startup: is_startup.clone(),
            account,
        }
    }
}


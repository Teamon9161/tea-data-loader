mod factor;
#[cfg(feature = "fac-ext")]
mod methods;
#[cfg(feature = "fac-ext")]
mod ops;

pub use factor::Factor;
#[cfg(feature = "fac-ext")]
pub use methods::*;

#[cfg(test)]
mod tests {
    #[cfg(feature = "order-book-fac")]
    use crate::factors::tick::order_book::*;
    use crate::factors::*;

    #[cfg(feature = "order-book-fac")]
    #[test]
    fn test_factor_add() {
        let factor1 = Factor::<Mid>::new(Param::None);
        let factor2 = Factor::<Ask>::new(1);
        let add_fac = factor1 + factor2;
        assert_eq!(&add_fac.name(), "mid + ask_1");
    }

    #[cfg(feature = "order-book-fac")]
    #[test]
    fn test_factor_sub() {
        let factor1 = Factor::<Mid>::new(Param::None);
        let factor2 = Factor::<Bid>::new(1);
        let sub_fac = factor1 - factor2;
        assert_eq!(&sub_fac.name(), "mid - bid_1");
    }

    #[cfg(feature = "order-book-fac")]
    #[test]
    fn test_factor_div() {
        let factor1 = Factor::<Ask>::new(1);
        let factor2 = Factor::<Bid>::new(1);
        let div_fac = factor1 / factor2;
        assert_eq!(&div_fac.name(), "ask_1 / bid_1");
    }

    #[cfg(feature = "order-book-fac")]
    #[test]
    fn test_factor_mul() {
        let factor1 = Factor::<Mid>::new(Param::None);
        let factor2 = Factor::<Ask1>::new(Param::None);
        let mul_fac = factor1 * factor2;
        assert_eq!(&mul_fac.name(), "mid * ask1");
    }
}

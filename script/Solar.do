clear
set more off
global path "F:\Core Resume Files\Job Applications\Fraym\Fraym_Case\data\STATA"
cd "$path"

*** RESHAPING/MODIFYING DATA

	use "hh/hh_sec_a.dta", clear
		drop hh_a03_1- hh_a27
		drop occ hh_a06 hh_a13 y4_rural
		drop hh_a02_1
	save "hh/hh_sec_a_mod.dta", replace
	
	use "hh/hh_sec_b.dta", clear
		drop hh_b05- hh_b29_3
		drop occ hh_b01-hh_b03_2
		
	reshape wide hh_b04, i(y4_hhid) j(indidy4)
	
	egen num_hh = rownonmiss(hh_b041- hh_b0433)
		label var num_hh "Number of household members"
	egen num_under5 = anycount(hh_b041- hh_b0433), values(0/5)
		label var num_under5 "Number of household members under five"
	egen num_over5 = anycount(hh_b041- hh_b0433), values(6/120)
		label var num_over5 "Number of household members six and over"
	
		drop hh_b04*
	save "hh/hh_sec_b_reshaped.dta", replace
	
	use "hh/hh_sec_b.dta", clear
		keep if indidy4 == 1
		keep y4_hhid hh_b05
	save "hh/hh_sec_b_mod.dta", replace
	
	use "hh/hh_sec_c.dta", clear
		keep if indidy4 == 1
		keep y4_hhid hh_c07
	save "hh/hh_sec_c_mod.dta", replace
	
	use "hh/hh_sec_e.dta", clear
		drop if hh_e01 == 2
		drop if hh_e24 == . 
	
	gen wage = . 
		replace wage = hh_e26_1 if hh_e26_2 == 8
		replace wage = hh_e26_1 * 2 if hh_e26_2 == 7
		replace wage = hh_e26_1 * 4 if hh_e26_2 == 6
		replace wage = hh_e26_1 * hh_e29 if hh_e26_2 == 5
		replace wage = hh_e26_1 / 2 * hh_e29* hh_e30 if hh_e26_2 == 4
		replace wage = hh_e26_1 * hh_e29* hh_e30 if hh_e26_2 == 3
		* Making (a conservative) assumption that people only work 5 days a week (which may not be the case for all people)
		replace wage = hh_e26_1 * hh_e29* hh_e30 * 5 if hh_e26_2 == 2
	order wage, after(hh_e26_2)
		
	collapse(sum) wage, by (y4_hhid)
	save "hh/hh_sec_e_mod.dta", replace
	
	
	use "hh/hh_sec_m.dta", clear
		drop hh_m02 hh_m03 hh_m04
		drop occ
	reshape wide hh_m01, i(y4_hhid) j(itemcode)
		
	* to install applyCodebook, see instructions here: https://github.com/worldbank/stata 
	applyCodebook using "applyCodebook.xlsx", rename varlab 
		*drop hh_m01416-hh_m01453
		keep y4_hhid refridgerator_or_freezer water_heater fan_airconditioner tv video_dvd computer telephone_mobile 

	save "hh/hh_sec_m_mod.dta", replace
	
	use "hh/consumptionnps4.dta", clear
		drop fisherb4c4- expmR
		drop adulteq intyear intmonth quarter month ea village ward district region
		drop cluster- hhsize
	
	egen consumption = rowtotal(foodbev- other)
	keep y4_hhid consumption utilities
	
	save "consumptionnps4_mod.dta", replace 
	
*** MERGING DATA
	use "hh/hh_sec_a_mod.dta", clear
	encode hh_a02_2, gen(hh_a02_1)
	order hh_a02_1, before(hh_a02_2)
	
	merge m:1 clusterid using "npsy4.ea.offset.dta"
		drop _merge
		order lat_modified lon_modified, after(clusterid)

	merge 1:1 y4_hhid using "hh/hh_sec_i.dta"
		drop _merge
		drop occ hh_i04- hh_i07_2 hh_i11- hh_i16 hh_i19- hh_i36
	
	merge 1:1 y4_hhid using "hh/hh_sec_b_reshaped.dta"
		drop _merge
	
	merge 1:1 y4_hhid using "hh/hh_sec_b_mod.dta"
		drop _merge
	
	merge 1:1 y4_hhid using "hh/hh_sec_c_mod.dta"
		drop _merge

	merge 1:1 y4_hhid using "hh/hh_sec_e_mod.dta"
		drop _merge
		
	merge 1:1 y4_hhid using "hh/hh_sec_m_mod.dta"
		drop _merge
	
	merge 1:1 y4_hhid using "consumptionnps4_mod.dta"
		drop _merge
		
	save "$path\final.dta", replace

*** GENERATING VARIABLES
	use "$path\final.dta", clear
	keep y4_hhid strataid clustertype y4_weights lat_modified lon_modified hh_a01_1 hh_a01_2 hh_a02_1 hh_a02_2 hh_a02_2 hh_i17 hh_i18 wage consumption utilities telephone_mobile-fan_airconditioner num_hh
	
	replace hh_a02_2 = proper(hh_a02_2)	
	
	* names of districts need to be consistent with those in the district shapefile in order to merge them
	gen district = hh_a02_2
		replace district = subinstr(district, "Arusha Mjini", "Arusha Urban", 1)
		replace district = subinstr(district, "Babati Mji", "Babati Urban", 1)
		replace district = subinstr(district, "Bukoba Manispaa", "Bukoba Urban", 1)
		replace district = subinstr(district, "Dodoma Mjini", "Dodoma Urban", 1)
		replace district = subinstr(district, "Ilemela Manispaa", "Ilemela", 1)
		replace district = subinstr(district, "Iringa Manispaa", "Iringa Urban", 1)
		replace district = subinstr(district, "Kahama Mji", "Kahama Township Authority", 1)
		replace district = subinstr(district, "Kasulu Mji", "Kasulu Township Authority", 1)
		replace district = subinstr(district, "Kigoma Manispaa", "Kigoma Urban", 1)
		replace district = subinstr(district, "Lindi Manispaa", "Lindi Urban", 1)
		replace district = subinstr(district, "Mafinga Mji", "Mafinga Township Authority", 1)
		replace district = subinstr(district, "Makambako Mji", "Makambako Township Authority", 1)
		replace district = subinstr(district, "Masasi Mji", "Masasi Township Authority", 1)
		replace district = subinstr(district, "Mbeya Jiji", "Mbeya Urban", 1)
		replace district = subinstr(district, "Morogoro Manispaa", "Morogoro Urban", 1)
		replace district = subinstr(district, "Moshi Manispaa", "Moshi Urban", 1)
		replace district = subinstr(district, "Mpanda Mji", "Mpanda Urban", 1)
		replace district = subinstr(district, "Mtwara Vijijini", "Mtwara", 1)
		replace district = subinstr(district, "Mtwara Manispaa", "Mtwara Urban", 1)
		replace district = subinstr(district, "Musoma Manispaa", "Musoma Urban", 1)
		replace district = subinstr(district, "Njombe Mji", "Njombe Urban", 1)
		replace district = subinstr(district, "Nyamagana Manispaa", "Nyamagana", 1)
		replace district = subinstr(district, "Shinyanga Manispaa", "Shinyanga Urban", 1)
		replace district = subinstr(district, "Singida Mjini", "Singida Urban", 1)
		replace district = subinstr(district, "Sumbawanga Manispaa", "Sumbawanga Urban", 1)
		replace district = subinstr(district, "Tabora Manispaa", "Tabora Urban", 1)
		replace district = subinstr(district, "Tanga", "Tanga Urban", 1)
		replace district = subinstr(district, "Wang'Ing'Ombe", "Wanging'ombe", 1)

	bysort clustertype: sum consumption
	
	sum consumption, detail
	sum consumption if consumption > 1673853.5, detail
	
	gen poor = 1 if consumption <= 1673853.5
		replace poor = 0 if poor == .
	gen low = 1 if consumption > 1673853.5 & consumption <= 3396333
		replace low = 0 if low == .
	gen middle = 1 if consumption > 3396333 & consumption <= 5569832
		replace middle = 0 if middle == .
	gen high = 1 if consumption > 5569832 & consumption != .
		replace high = 0 if high == .

	tab hh_i17, gen(fuel)
		rename fuel1 elec
		rename fuel2 solar
		rename fuel3 lampoil
		rename fuel4 candle
		rename fuel5 firewood
		rename fuel6 generator
		rename fuel7 torch
		rename fuel8 other
	
	gen rural = (clustertype == 1)
	gen urban = (clustertype == 2)
	
	sum utilities
	
	gen high_util = (utilities > r(mean))
	
	gen customer0 = 1 if hh_i17 == 2 |(fan_airconditioner == 0 & refridgerator_or_freezer == 0 & tv == 0 & computer == 0 & water_heater == 0 & telephone_mobile == 0)
	gen customer1 = 1 if (telephone_mobile >= 1 & telephone_mobile <= 2) & fan_airconditioner == 0 & refridgerator_or_freezer == 0 & tv == 0 & computer == 0 & water_heater == 0 & hh_i17 != 2
	gen customer2 = 1 if (telephone_mobile >= 3 & telephone_mobile <= 5) & (tv > 0 | computer > 0)& fan_airconditioner == 0 & refridgerator_or_freezer == 0 & water_heater == 0 & hh_i17 != 2

	
	gen customer3 = 1 if (fan_airconditioner > 0 | refridgerator_or_freezer > 0 | water_heater > 0) & hh_i17 != 2
		replace customer2 = 1 if customer2 == . & (tv > 0 | computer >0) & hh_i17 != 2 & customer3 != 1 & customer1 != 1
		replace customer2 = 1 if customer2 == . & telephone_mobile >= 3 & fan_airconditioner == 0 & refridgerator_or_freezer == 0 & tv == 0 & computer == 0 & water_heater == 0 & hh_i17 != 2
		
	replace customer1 = 0 if customer1 == . 
	replace customer2 = 0 if customer2 == . 
	replace customer3 = 0 if customer3 == . 
	
	egen row_sum = rowtotal(customer*)
	egen row_miss = rowmiss(customer*)
		
	save "$path\simple.dta", replace

*** FOR MAPS
	use "$path\simple.dta", clear
	collapse (mean) solar elec, by(district)
		replace solar = solar * 100
		replace elec = elec * 100
	outsheet using "solar_elec2.csv", comma replace
	
	use "$path\simple.dta", clear
	collapse (mean) solar elec, by(district urban)
	
	bysort urban: sum solar
	bysort urban: sum elec
	
	use "$path\simple.dta", clear

*** MARKET SIZE AND REVENUE	
	use "$path\simple.dta", clear
	collapse (count) hh_a02_1, by(district)
	gen prop = hh_a02_1/3352
	gen hh = round(prop* (53879957/5.0))
	save "prop.dta", replace
	
	use "$path\simple.dta", clear
	collapse (mean) customer1, by(district) 
	save "customer1.dta", replace
	
	use "$path\simple.dta", clear
	collapse (mean) customer2, by(district) 
	save "customer2.dta", replace
	
	use "$path\simple.dta", clear
	collapse (mean) customer3, by(district) 
	save "customer3.dta", replace
	
	use "prop.dta", clear
	merge 1:1 district using "customer1.dta"
		drop _merge
		
	merge 1:1 district using "customer2.dta"
		drop _merge
	merge 1:1 district using "customer3.dta"
		drop _merge
	
	sort customer1 customer2 customer3
	
	gen customer1_num = customer1 * hh
	gen customer2_num = customer2 * hh
	gen customer3_num = customer3 * hh
	
	egen tot_num = rowtotal(customer*_num)
	
	egen tot_size1 = sum(customer1_num)
	egen tot_size2 = sum(customer2_num)
	egen tot_size3 = sum(customer3_num)
	
	format tot_size1 %14.0fc
	format tot_size2 %14.0fc
	format tot_size3 %14.0fc
	
	gen customer1_rev = customer1_num * 50
	gen customer2_rev = customer2_num * 100
	gen customer3_rev = customer3_num * 250
	
	egen tot_revenue = rowtotal(customer*_rev)
	sort tot_revenue
	format tot_revenue %14.0fc 
	
	save "marketsize.dta", replace

*** FOR REVENUE MAP
	use "marketsize.dta", clear
	keep district tot_revenue
	outsheet using "tot_revenue.csv", comma replace

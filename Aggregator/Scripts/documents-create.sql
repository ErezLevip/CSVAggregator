CREATE TABLE `documents` (
  `Id` varchar(36) NOT NULL,
  `WorkerId` varchar(36) NOT NULL,
  `Processed` date NOT NULL,
  `AggregateFiles` varchar(2000) NOT NULL,
  `Data` varchar(2000) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1

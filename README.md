# racktables2netbox
A [RackTables](https://github.com/racktables/racktables) to [NetBox](https://github.com/digitalocean/netbox) migration utility. This tiny tool should be used to migrate your existing RackTables installations towards NetBox.

## Installation
```curl --output racktables2netbox.zip https://github.com/goebelmeier/racktables2netbox/archive/master.zip
unzip racktables2netbox.zip
cd racktables2netbox
cp conf.sample conf
```

## Usage
1. Create a NetBox API Token
2. Create a RackTables read-only database user
3. edit ``conf`` regarding your needs (URLs, credentials, ...)
4. run `python3 racktables2netbox.py`
5. optional: to get back to a clean NetBox installation run `python3 clean_netbox.py`

## Contributing
1. Migration should follow a strict order. Please have a look at the corresponding [wiki page](https://github.com/goebelmeier/racktables2netbox/wiki/Migration-order)
1. Fork it (<https://github.com/yourname/yourproject/fork>)
2. Create your feature branch (`git checkout -b feature/fooBar`)
3. Commit your changes (`git commit -am 'Add some fooBar'`)
4. Push to the branch (`git push origin feature/fooBar`)
5. Create a new Pull Request

## Credits
Thanks to [Device42](https://www.device42.com/) who have already written a [RackTables to Device42 migration utility](https://github.com/device42/Racktables-to-Device42-Migration). I was able to use it as a starting point and begin to rewrite it step by step towards NetBox.

## License
racktables2netbox is licensed under MIT license. See [LICENSE.md](LICENSE.md) for more information.
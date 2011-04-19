from filmdata.lib.plugin_manager import PluginManager

manager = PluginManager('filmdata.sources', __file__,
                        ('Fetch', 'Produce', 'schema'))

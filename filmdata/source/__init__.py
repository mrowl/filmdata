from filmdata.lib.plugin_manager import PluginManager

manager = PluginManager('filmdata.source', __file__,
                        ('Fetch', 'Produce', 'schema'))

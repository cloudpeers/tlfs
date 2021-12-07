import 'package:flutter/material.dart';
import 'package:flutter/services.dart';
import 'package:path/path.dart';
import 'package:path_provider/path_provider.dart';
import './tlfs.dart' as tlfs;

/// Opens or creates a db at `~/Documents/{appname}`, loads the schema
/// package from `assets/{appname}.tlfs.rkyv and initializes the sdk.
Future<tlfs.Sdk> _loadSdk(String appname) async {
  final documentsDirectory = await getApplicationDocumentsDirectory();
  final dbPath = join(documentsDirectory.path, appname, 'db');
  final assetName =
      'assets/{appname}.tlfs.rkyv'.replaceAll('{appname}', appname);
  final schema = await rootBundle.load(assetName);
  return tlfs.Sdk.createPersistent(tlfs.Api.load(), dbPath, schema.buffer.asUint8List());
}

class _InheritedSdk extends InheritedWidget {
  const _InheritedSdk({
    Key? key,
    required this.data,
    required Widget child,
  }) : super(key: key, child: child);

  final SdkState data;

  @override
  bool updateShouldNotify(InheritedWidget oldWidget) => false;
}

class Sdk extends StatefulWidget {
  const Sdk({
    Key? key,
    required this.appname,
    required this.child,
    this.debug = false,
    this.debugError,
  }) : super(key: key);

  final String appname;
  final Widget child;

  final bool debug;
  final String? debugError;

  @override
  SdkState createState() => SdkState();

  static SdkState of(BuildContext context) {
    final _InheritedSdk? result =
        context.dependOnInheritedWidgetOfExactType<_InheritedSdk>();
    assert(result != null, 'No Sdk found in context');
    return result!.data;
  }
}

class SdkState extends State<Sdk> {
  tlfs.Sdk? _sdk;
  String? _err;

  tlfs.Sdk get sdk => _sdk!;

  @override
  initState() {
    if (widget.debug == true) {
      _err = widget.debugError;
      return;
    }
    _loadSdk(widget.appname).then((sdk) {
      setState(() {
        _sdk = sdk;
      });
    }).catchError((err) {
      setState(() {
        _err = err.toString();
      });
    });
    super.initState();
  }

  @override
  Widget build(BuildContext context) {
    if (_sdk != null) {
      return _InheritedSdk(
        data: this,
        child: widget.child,
      );
    } else if (_err != null) {
      return SdkError(msg: _err!);
    } else {
      return SdkLoading();
    }
  }

  @override
  dispose() {
    if (_sdk != null) {
      _sdk!.drop();
    }
    super.dispose();
  }
}

class SdkLoading extends StatelessWidget {
  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      home: Scaffold(
        body: Center(
          child: Column(
            mainAxisAlignment: MainAxisAlignment.center,
            crossAxisAlignment: CrossAxisAlignment.center,
            children: [
              SizedBox(
                child: CircularProgressIndicator(),
                width: 60,
                height: 60,
              ),
              Padding(
                padding: EdgeInsets.only(top: 16),
                child: Text('Loading sdk...'),
              )
            ],
          ),
        ),
      ),
    );
  }
}

class SdkError extends StatelessWidget {
  const SdkError({
    Key? key,
    required this.msg,
  }) : super(key: key);

  final String msg;

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      home: Scaffold(
        body: Center(
          child: Column(
            mainAxisAlignment: MainAxisAlignment.center,
            crossAxisAlignment: CrossAxisAlignment.center,
            children: [
              Text('Error loading sdk...'),
              Padding(
                padding: EdgeInsets.only(top: 16),
                child: Text(msg),
              )
            ],
          ),
        ),
      ),
    );
  }
}
